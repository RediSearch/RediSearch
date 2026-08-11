/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Lowering: turn a decoded [`Sequence`] AST into concrete commands. This is the
//! stateful half of the grammar — it keeps a live model of created indexes,
//! keys, dicts, and aliases, and resolves the AST's abstract `u16` references
//! against it (`index % live_count`, `field % index.fields.len()`), so queries
//! reference things that actually exist and reach deep execution. The AST
//! (`ast.rs`) stays purely declarative; all the "make it valid" logic is here.

use arbitrary::{Arbitrary, Unstructured};

use crate::ast::*;
use crate::op::{Args, Cmd};
use crate::shadow::{FieldKind, IndexModel, VecType};

/// `$PARAMS` bindings: `(name, value)` pairs (e.g. a KNN query's `BLOB`).
type Params = Vec<(Vec<u8>, Vec<u8>)>;

/// Decode a byte buffer into up to `max_cmds` concrete commands. Commands are
/// decoded one at a time (not as a `Vec`, whose `Arbitrary` self-limits length),
/// so the sequence length is driven by `max_cmds` and the buffer size.
///
/// `malform_rate` corrupts roughly 1-in-`rate` rendered commands at the envelope
/// level (bad counts, dropped/duplicated args, ...) to stress the argument
/// parsers; 0 disables it. Corruption is byte-driven, so it replays exactly.
pub fn decode(
    bytes: &[u8],
    max_cmds: usize,
    json_available: bool,
    safe_utf8: bool,
    malform_rate: u32,
) -> Vec<Cmd> {
    let mut u = Unstructured::new(bytes);
    let mut model = Model::new(json_available, safe_utf8);
    let mut out = Vec::new();
    while out.len() < max_cmds && !u.is_empty() {
        let cmd = match Command::arbitrary(&mut u) {
            Ok(cmd) => cmd,
            Err(_) => break,
        };
        // Decode the (possible) malform + gate from the same stream so it is
        // deterministic and part of the byte→sequence mapping.
        let malform = Malform::arbitrary(&mut u).ok();
        let apply = malform_rate > 0
            && u.int_in_range(0..=malform_rate.saturating_sub(1))
                .unwrap_or(1)
                == 0;

        let before = out.len();
        model.lower(cmd, &mut out);
        if apply {
            if let Some(m) = &malform {
                for c in out[before..].iter_mut() {
                    *c = apply_malform(m, std::mem::take(c), safe_utf8);
                }
            }
        }
    }
    out.truncate(max_cmds);
    out
}

/// Render a `Garbage` token to bytes (raw binary only when `!safe_utf8`).
fn garbage_bytes(g: &Garbage, safe_utf8: bool) -> Vec<u8> {
    match g {
        Garbage::At => b"@".to_vec(),
        Garbage::OpenBracket => b"[".to_vec(),
        Garbage::OpenParens => b"(((".to_vec(),
        Garbage::DanglingKnn => b"=>[KNN".to_vec(),
        Garbage::Quote => b"\"".to_vec(),
        Garbage::Pipe => b"|||".to_vec(),
        Garbage::EmptyTag => b"@field:{".to_vec(),
        Garbage::EmptyRange => b"@n:[a b]".to_vec(),
        Garbage::Binary(bytes) if !safe_utf8 && !bytes.is_empty() => {
            bytes.iter().map(|b| b | 1).collect()
        }
        Garbage::Binary(_) => b"@x".to_vec(),
    }
}

/// Apply a structural corruption to a rendered command's argument vector.
fn apply_malform(m: &Malform, mut cmd: Cmd, safe_utf8: bool) -> Cmd {
    if cmd.is_empty() {
        return cmd;
    }
    let len = cmd.len();
    match m {
        Malform::DropArg(i) => {
            cmd.remove(usize::from(*i) % len);
        }
        Malform::DupArg(i) => {
            let idx = usize::from(*i) % len;
            cmd.insert(idx, cmd[idx].clone());
        }
        Malform::Truncate(i) => {
            cmd.truncate(1 + usize::from(*i) % len);
        }
        Malform::InsertGarbage(i, g) => {
            cmd.insert(usize::from(*i) % (len + 1), garbage_bytes(g, safe_utf8));
        }
        Malform::BadCount(i, n) => {
            // Prefer replacing a numeric-looking arg (a count/limit/dim), else
            // the arg at idx — either way the command's structure no longer adds up.
            let numeric: Vec<usize> = (0..len)
                .filter(|&j| {
                    std::str::from_utf8(&cmd[j])
                        .ok()
                        .and_then(|s| s.parse::<i64>().ok())
                        .is_some()
                })
                .collect();
            let idx = if numeric.is_empty() {
                usize::from(*i) % len
            } else {
                numeric[usize::from(*i) % numeric.len()]
            };
            cmd[idx] = n.as_str().into_bytes();
        }
        Malform::SwapAdjacent(i) if len >= 2 => {
            let idx = usize::from(*i) % (len - 1);
            cmd.swap(idx, idx + 1);
        }
        Malform::SwapAdjacent(_) => {}
        Malform::RepeatAll => {
            cmd.extend_from_within(..);
        }
        Malform::ExtraArgs(gs) => {
            for g in gs.iter().take(4) {
                cmd.push(garbage_bytes(g, safe_utf8));
            }
        }
    }
    cmd
}

struct Model {
    indexes: Vec<IndexModel>,
    keys: Vec<String>,
    dicts: Vec<String>,
    aliases: Vec<String>,
    next_id: u64,
    json_available: bool,
    safe_utf8: bool,
}

impl Model {
    fn new(json_available: bool, safe_utf8: bool) -> Model {
        Model {
            indexes: Vec::new(),
            keys: Vec::new(),
            dicts: Vec::new(),
            aliases: Vec::new(),
            next_id: 0,
            json_available,
            safe_utf8,
        }
    }

    fn lower(&mut self, cmd: Command, out: &mut Vec<Cmd>) {
        match cmd {
            Command::Create(spec) => self.create(spec, out),
            Command::Alter { index, field } => self.alter(index, field, out),
            Command::Hset { index, key, values } => self.hset(index, key, values, out),
            Command::JsonSet { index, key } => self.json_set(index, key, out),
            Command::Del { key } => out.push(Args::cmd("DEL").kw(&self.key(key)).build()),
            Command::FtDel { index, key } => {
                if let Some(im) = self.resolve_index(index) {
                    out.push(Args::cmd("FT.DEL").kw(&im.name).kw(&self.key(key)).build());
                }
            }
            Command::Search(spec) => self.search(spec, out),
            Command::Aggregate(spec) => self.aggregate(spec, out),
            Command::CursorRead { index, cursor } => self.cursor("READ", index, cursor, out),
            Command::CursorDel { index, cursor } => self.cursor("DEL", index, cursor, out),
            Command::DictAdd { dict, terms } => {
                let name = format!("dict{}", dict % 8);
                if !self.dicts.contains(&name) {
                    self.dicts.push(name.clone());
                }
                let a = Args::cmd("FT.DICTADD").kw(&name).extend(words(&terms));
                out.push(a.build());
            }
            Command::SynUpdate {
                index,
                group,
                terms,
            } => {
                if let Some(im) = self.resolve_index(index) {
                    let a = Args::cmd("FT.SYNUPDATE")
                        .kw(&im.name)
                        .val(group % 8)
                        .extend(words(&terms));
                    out.push(a.build());
                }
            }
            Command::DropIndex { index, keep_docs } => {
                if let Some(im) = self.take_index(index) {
                    let a = Args::cmd("FT.DROPINDEX")
                        .kw(&im.name)
                        .when(!keep_docs, |a| a.kw("DD"));
                    out.push(a.build());
                }
            }
            Command::AliasAdd { alias, index } => {
                self.alias("FT.ALIASADD", alias, Some(index), out)
            }
            Command::AliasUpdate { alias, index } => {
                self.alias("FT.ALIASUPDATE", alias, Some(index), out)
            }
            Command::AliasDel { alias } => self.alias("FT.ALIASDEL", alias, None, out),
            Command::TagVals { index, field } => {
                if let Some(im) = self.resolve_index(index) {
                    let f = pick_field_of(&im, FieldKind::Tag, field)
                        .or_else(|| im.fields.first().map(|(n, _)| n.clone()));
                    if let Some(f) = f {
                        out.push(Args::cmd("FT.TAGVALS").kw(&im.name).kw(&f).build());
                    }
                }
            }
            Command::Explain { index, query } => {
                if let Some(im) = self.resolve_index(index) {
                    let (q, _) = self.query(&im, &query);
                    out.push(
                        Args::cmd("FT.EXPLAIN")
                            .kw(&im.name)
                            .raw(q)
                            .kw("DIALECT")
                            .kw("2")
                            .build(),
                    );
                }
            }
            Command::Profile {
                index,
                aggregate,
                query,
            } => {
                if let Some(im) = self.resolve_index(index) {
                    let (q, params) = self.query(&im, &query);
                    let a = Args::cmd("FT.PROFILE")
                        .kw(&im.name)
                        .kw(if aggregate { "AGGREGATE" } else { "SEARCH" })
                        .kw("QUERY")
                        .raw(q)
                        .kw("DIALECT")
                        .kw("2")
                        .extend(render_params(params));
                    out.push(a.build());
                }
            }
            Command::SpellCheck {
                index,
                query,
                distance,
            } => {
                if let Some(im) = self.resolve_index(index) {
                    let (q, _) = self.query(&im, &query);
                    let a = Args::cmd("FT.SPELLCHECK")
                        .kw(&im.name)
                        .raw(q)
                        .kw("DISTANCE")
                        .val(1 + u32::from(distance % 4));
                    out.push(a.build());
                }
            }
            Command::SugAdd { key, term, score } => {
                out.push(
                    Args::cmd("FT.SUGADD")
                        .kw(&format!("sug{}", key % 8))
                        .kw(term.as_str())
                        .val(1 + u32::from(score % 100))
                        .build(),
                );
            }
            Command::SugGet { key, prefix, fuzzy } => {
                out.push(
                    Args::cmd("FT.SUGGET")
                        .kw(&format!("sug{}", key % 8))
                        .kw(prefix.as_str())
                        .when(fuzzy, |a| a.kw("FUZZY"))
                        .build(),
                );
            }
            Command::SugLen { key } => {
                out.push(
                    Args::cmd("FT.SUGLEN")
                        .kw(&format!("sug{}", key % 8))
                        .build(),
                );
            }
            Command::SugDel { key, term } => {
                out.push(
                    Args::cmd("FT.SUGDEL")
                        .kw(&format!("sug{}", key % 8))
                        .kw(term.as_str())
                        .build(),
                );
            }
            Command::Mget { index, keys } => {
                if let Some(im) = self.resolve_index(index) {
                    let a = keys
                        .iter()
                        .take(6)
                        .fold(Args::cmd("FT.MGET").kw(&im.name), |a, k| {
                            a.kw(&format!("{}{k}", im.prefix))
                        });
                    out.push(a.build());
                }
            }
            Command::DictDel { dict, terms } => {
                let name = format!("dict{}", dict % 8);
                let a = Args::cmd("FT.DICTDEL").kw(&name).extend(words(&terms));
                out.push(a.build());
            }
            Command::SynDump { index } => {
                if let Some(im) = self.resolve_index(index) {
                    out.push(Args::cmd("FT.SYNDUMP").kw(&im.name).build());
                }
            }
            Command::ConfigSet { param, value } => {
                out.push(
                    Args::cmd("CONFIG")
                        .kw("SET")
                        .kw(param.as_str())
                        .kw(&value.as_str())
                        .build(),
                );
            }
            Command::ConfigGet { param } => {
                out.push(Args::cmd("CONFIG").kw("GET").kw(param.as_str()).build());
            }
            Command::Debug { index, sub } => self.debug(index, sub, out),
        }
    }

    fn debug(&mut self, index: u16, sub: DebugSub, out: &mut Vec<Cmd>) {
        let Some(im) = self.resolve_index(index) else {
            return;
        };
        // Resolve a field ref of any kind (dumps often error on the wrong kind,
        // which is itself worth exercising).
        let field = |r: u16| {
            pick_field(&im, r)
                .map(|(n, _)| n)
                .unwrap_or_else(|| "f".to_string())
        };
        let a = Args::cmd("FT.DEBUG");
        let a = match &sub {
            DebugSub::GcForceInvoke => a.kw("GC_FORCEINVOKE").kw(&im.name),
            DebugSub::GcForceBgInvoke => a.kw("GC_FORCEBGINVOKE").kw(&im.name),
            DebugSub::GcCleanNumeric => a.kw("GC_CLEAN_NUMERIC").kw(&im.name),
            DebugSub::GcWaitForJobs => a.kw("GC_WAIT_FOR_JOBS"),
            DebugSub::GcStopSchedule => a.kw("GC_STOP_SCHEDULE").kw(&im.name),
            DebugSub::GcContinueSchedule => a.kw("GC_CONTINUE_SCHEDULE").kw(&im.name),
            DebugSub::DumpInvidx { term } => a.kw("DUMP_INVIDX").kw(&im.name).kw(term.as_str()),
            DebugSub::DumpNumidx { field: r } => a.kw("DUMP_NUMIDX").kw(&im.name).kw(&field(*r)),
            DebugSub::DumpNumidxTree { field: r } => {
                a.kw("DUMP_NUMIDXTREE").kw(&im.name).kw(&field(*r))
            }
            DebugSub::DumpTagidx { field: r } => a.kw("DUMP_TAGIDX").kw(&im.name).kw(&field(*r)),
            DebugSub::DumpTerms => a.kw("DUMP_TERMS").kw(&im.name),
            DebugSub::DumpHnsw { field: r } => a.kw("DUMP_HNSW").kw(&im.name).kw(&field(*r)),
            DebugSub::DumpGeomidx { field: r } => a.kw("DUMP_GEOMIDX").kw(&im.name).kw(&field(*r)),
            DebugSub::DumpPrefixTrie => a.kw("DUMP_PREFIX_TRIE").kw(&im.name),
            DebugSub::DumpSuffixTrie => a.kw("DUMP_SUFFIX_TRIE").kw(&im.name),
            DebugSub::DumpPhoneticHash { term } => a.kw("DUMP_PHONETIC_HASH").kw(term.as_str()),
            DebugSub::DumpSchema => a.kw("DUMP_SCHEMA").kw(&im.name),
            DebugSub::DumpDeletedIds => a.kw("DUMP_DELETED_IDS").kw(&im.name),
            DebugSub::InvidxSummary { term } => {
                a.kw("INVIDX_SUMMARY").kw(&im.name).kw(term.as_str())
            }
            DebugSub::NumidxSummary { field: r } => {
                a.kw("NUMIDX_SUMMARY").kw(&im.name).kw(&field(*r))
            }
            DebugSub::InfoTagidx { field: r } => a.kw("INFO_TAGIDX").kw(&im.name).kw(&field(*r)),
            DebugSub::IdToDocid { id } => a.kw("IDTODOCID").kw(&im.name).kw(&id.as_str()),
            DebugSub::DocidToId { key } => a.kw("DOCIDTOID").kw(&im.name).kw(&self.key(*key)),
            DebugSub::DocInfo { key } => a.kw("DOCINFO").kw(&im.name).kw(&self.key(*key)),
            DebugSub::GetMaxDocId => a.kw("GET_MAX_DOC_ID").kw(&im.name),
        };
        out.push(a.build());
    }

    // ---- FT.CREATE / FT.ALTER ----------------------------------------------

    fn create(&mut self, spec: CreateSpec, out: &mut Vec<Cmd>) {
        let name = format!("idx{}", self.next_id);
        self.next_id += 1;
        let on_json = spec.on_json && self.json_available;
        let prefix = "doc:".to_string();

        // Always keep "doc:" so generated keys get indexed; add extra prefixes.
        let mut prefixes = vec![prefix.clone()];
        for w in spec.prefixes.iter().take(3) {
            prefixes.push(format!("{}:", w.as_str()));
        }

        let mut a =
            Args::cmd("FT.CREATE")
                .kw(&name)
                .kw("ON")
                .kw(if on_json { "JSON" } else { "HASH" });
        a = a.kw("PREFIX").val(prefixes.len());
        for p in &prefixes {
            a = a.kw(p);
        }
        for opt in &spec.options {
            a = render_create_opt(a, opt);
        }
        a = a.kw("SCHEMA");

        let mut model_fields: Vec<(String, FieldKind)> = Vec::new();
        for fs in spec.fields.iter().take(8) {
            let fname = fs.name.as_str().to_string();
            if model_fields.iter().any(|(n, _)| n == &fname) {
                continue; // names unique within an index
            }
            let (na, kind) = render_field(a, fs, on_json);
            a = na;
            model_fields.push((fname, kind));
        }
        if model_fields.is_empty() {
            a = a.kw("title").kw("TEXT");
            model_fields.push(("title".to_string(), FieldKind::Text));
        }

        out.push(a.build());
        self.indexes.push(IndexModel {
            name,
            on_json,
            prefix,
            fields: model_fields,
        });
    }

    fn alter(&mut self, index: u16, field: FieldSpec, out: &mut Vec<Cmd>) {
        let Some(pos) = self.index_pos(index) else {
            return;
        };
        let on_json = self.indexes[pos].on_json;
        let fname = field.name.as_str().to_string();
        let a = Args::cmd("FT.ALTER")
            .kw(&self.indexes[pos].name)
            .kw("SCHEMA")
            .kw("ADD");
        let (a, kind) = render_field(a, &field, on_json);
        out.push(a.build());
        if !self.indexes[pos].fields.iter().any(|(n, _)| n == &fname) {
            self.indexes[pos].fields.push((fname, kind));
        }
    }

    // ---- documents ---------------------------------------------------------

    fn hset(&mut self, index: u16, key: u8, values: Vec<ValueSpec>, out: &mut Vec<Cmd>) {
        let Some(im) = self.resolve_index(index) else {
            return;
        };
        let key = format!("{}{}", im.prefix, key);
        let mut a = Args::cmd("HSET").kw(&key);
        for v in values.iter().take(4) {
            let Some((fname, kind)) = pick_field(&im, v.field) else {
                continue;
            };
            a = a.kw(&fname).raw(hash_value(&kind, v));
        }
        if values.is_empty() {
            a = a.kw("title").raw(b"hello world".to_vec());
        }
        if !self.keys.contains(&key) {
            self.keys.push(key.clone());
        }
        out.push(a.build());
    }

    fn json_set(&mut self, index: u16, key: u8, out: &mut Vec<Cmd>) {
        let Some(im) = self.resolve_index(index) else {
            return;
        };
        if !im.on_json {
            return;
        }
        let key = format!("{}{}", im.prefix, key);
        let mut map = serde_json::Map::new();
        for (fname, kind) in &im.fields {
            map.insert(fname.clone(), json_value(kind));
        }
        let value = serde_json::Value::Object(map).to_string();
        if !self.keys.contains(&key) {
            self.keys.push(key.clone());
        }
        out.push(Args::cmd("JSON.SET").kw(&key).kw("$").kw(&value).build());
    }

    // ---- FT.SEARCH / FT.AGGREGATE ------------------------------------------

    fn search(&mut self, spec: SearchSpec, out: &mut Vec<Cmd>) {
        let Some(im) = self.resolve_index(spec.index) else {
            return;
        };
        let (query, params) = self.query(&im, &spec.query);
        let mut a = Args::cmd("FT.SEARCH").kw(&im.name).raw(query);
        for opt in spec.options.iter().take(8) {
            a = render_search_opt(a, opt, &im);
        }
        a = a.extend(render_params(params));
        out.push(a.build());
    }

    fn aggregate(&mut self, spec: AggSpec, out: &mut Vec<Cmd>) {
        let Some(im) = self.resolve_index(spec.index) else {
            return;
        };
        let (query, params) = self.query(&im, &spec.query);
        let mut a = Args::cmd("FT.AGGREGATE").kw(&im.name).raw(query);
        for step in spec.steps.iter().take(6) {
            a = render_agg_step(a, step, &im);
        }
        if !spec.load.is_empty() {
            a = a.kw("LOAD").val(spec.load.len().min(4));
            for r in spec.load.iter().take(4) {
                if let Some((n, _)) = pick_field(&im, *r) {
                    a = a.kw(&format!("@{n}"));
                }
            }
        }
        if let Some(c) = &spec.cursor {
            a = a.kw("WITHCURSOR");
            if let Some(n) = &c.count {
                a = a.kw("COUNT").kw(&n.as_str());
            }
            if let Some(n) = &c.max_idle {
                a = a.kw("MAXIDLE").kw(&n.as_str());
            }
        }
        a = a
            .kw("DIALECT")
            .kw(spec.dialect.as_str())
            .extend(render_params(params));
        out.push(a.build());
    }

    fn cursor(&mut self, sub: &str, index: u16, cursor: u16, out: &mut Vec<Cmd>) {
        if let Some(im) = self.resolve_index(index) {
            // No live cursor tracking here; the executor rewrites the id from the
            // cursors it actually observed. This is a plausible fallback id.
            out.push(
                Args::cmd("FT.CURSOR")
                    .kw(sub)
                    .kw(&im.name)
                    .val(cursor)
                    .build(),
            );
        }
    }

    fn alias(&mut self, cmd: &str, alias: u8, index: Option<u16>, out: &mut Vec<Cmd>) {
        let name = format!("alias{}", alias % 8);
        let a = Args::cmd(cmd).kw(&name);
        let a = match index.and_then(|i| self.resolve_index(i)) {
            Some(im) => a.kw(&im.name),
            None if cmd == "FT.ALIASDEL" => a,
            None => return,
        };
        if !self.aliases.contains(&name) {
            self.aliases.push(name);
        }
        out.push(a.build());
    }

    // ---- query lowering ----------------------------------------------------

    /// Render a query to bytes plus any `$PARAMS` bindings (vector blobs).
    fn query(&self, im: &IndexModel, q: &Query) -> (Vec<u8>, Params) {
        let mut params = Vec::new();
        let bytes = match q {
            Query::Plain { head, rest } => {
                let mut s = self.atom(im, head);
                for (join, atom) in rest.iter().take(4) {
                    s.extend_from_slice(join_sep(join));
                    s.extend(self.atom(im, atom));
                }
                s
            }
            Query::Knn(spec) => match im.vector_field() {
                Some((f, vt, dim)) => {
                    params.push((b"BLOB".to_vec(), blob(vt, dim, &[])));
                    let pre = match &spec.prefilter {
                        Prefilter::Everything => "*".to_string(),
                        Prefilter::Text(w) => match pick_field_of(im, FieldKind::Text, 0) {
                            Some(tf) => format!("@{tf}:{}", w.as_str()),
                            None => "*".to_string(),
                        },
                        Prefilter::Numeric(r) => match pick_field_of(im, FieldKind::Numeric, 0) {
                            Some(nf) => format!("@{nf}:{}", num_range(r)),
                            None => "*".to_string(),
                        },
                    };
                    let ef = spec
                        .ef_runtime
                        .map(|e| format!(" EF_RUNTIME {e}"))
                        .unwrap_or_default();
                    format!("{pre}=>[KNN {} @{f} $BLOB{ef} AS __v]", spec.k.as_str()).into_bytes()
                }
                None => b"*".to_vec(),
            },
            Query::VectorRange { radius } => match im.vector_field() {
                Some((f, vt, dim)) => {
                    params.push((b"BLOB".to_vec(), blob(vt, dim, &[])));
                    format!("@{f}:[VECTOR_RANGE {radius} $BLOB]").into_bytes()
                }
                None => b"*".to_vec(),
            },
        };
        (bytes, params)
    }

    fn atom(&self, im: &IndexModel, atom: &Atom) -> Vec<u8> {
        match atom {
            Atom::Everything => b"*".to_vec(),
            Atom::Term(w) => w.as_str().into(),
            Atom::TextField { field, word } => match pick_field_of(im, FieldKind::Text, *field) {
                Some(f) => format!("@{f}:{}", word.as_str()).into_bytes(),
                None => word.as_str().into(),
            },
            Atom::Phrase(a, b) => format!("\"{} {}\"", a.as_str(), b.as_str()).into_bytes(),
            Atom::Optional(w) => format!("~{}", w.as_str()).into_bytes(),
            Atom::Numeric {
                field,
                coerce,
                range,
            } => match pick_typed(im, FieldKind::Numeric, *field, *coerce) {
                Some(f) => format!("@{f}:{}", num_range(range)).into_bytes(),
                None => b"*".to_vec(),
            },
            Atom::Tag {
                field,
                coerce,
                values,
            } => match pick_typed(im, FieldKind::Tag, *field, *coerce) {
                Some(f) => {
                    let vs: Vec<&str> = if values.is_empty() {
                        vec!["x"]
                    } else {
                        values.iter().take(4).map(Word::as_str).collect()
                    };
                    format!("@{f}:{{{}}}", vs.join("|")).into_bytes()
                }
                None => b"*".to_vec(),
            },
            Atom::Geo {
                field,
                coerce,
                spec,
            } => match pick_typed(im, FieldKind::Geo, *field, *coerce) {
                Some(f) => format!("@{f}:[{}]", geo_spec(spec)).into_bytes(),
                None => b"*".to_vec(),
            },
            Atom::Affix(kind, w) => {
                let w = w.as_str();
                match kind {
                    Affix::Prefix => format!("{w}*"),
                    Affix::Suffix => format!("*{w}"),
                    Affix::Contains => format!("*{w}*"),
                    Affix::Fuzzy => format!("%{w}%"),
                    Affix::Wildcard => format!("w'{w}*'"),
                }
                .into_bytes()
            }
            Atom::Garbage(g) => self.garbage(g),
        }
    }

    fn garbage(&self, g: &Garbage) -> Vec<u8> {
        garbage_bytes(g, self.safe_utf8)
    }

    // ---- reference resolution ----------------------------------------------

    /// Resolve an index reference against the live model. Returns `None` before
    /// any index exists, so index-taking commands no-op early in a sequence (the
    /// fuzzer quickly learns to emit `FT.CREATE` first).
    fn resolve_index(&self, r: u16) -> Option<IndexModel> {
        self.index_pos(r).map(|p| self.indexes[p].clone())
    }

    fn index_pos(&self, r: u16) -> Option<usize> {
        if self.indexes.is_empty() {
            None
        } else {
            Some(usize::from(r) % self.indexes.len())
        }
    }

    fn take_index(&mut self, r: u16) -> Option<IndexModel> {
        if self.indexes.is_empty() {
            return None;
        }
        let pos = usize::from(r) % self.indexes.len();
        Some(self.indexes.remove(pos))
    }

    fn key(&self, k: u8) -> String {
        format!("doc:{k}")
    }
}

// ---- free rendering helpers ------------------------------------------------

fn render_create_opt(a: Args, opt: &CreateOpt) -> Args {
    match opt {
        CreateOpt::MaxTextFields => a.kw("MAXTEXTFIELDS"),
        CreateOpt::NoOffsets => a.kw("NOOFFSETS"),
        CreateOpt::NoHighlight => a.kw("NOHL"),
        CreateOpt::NoFields => a.kw("NOFIELDS"),
        CreateOpt::NoFreqs => a.kw("NOFREQS"),
        CreateOpt::SkipInitialScan => a.kw("SKIPINITIALSCAN"),
        CreateOpt::StopWords(ws) => {
            let a = a.kw("STOPWORDS").val(ws.len().min(4));
            ws.iter().take(4).fold(a, |a, w| a.kw(w.as_str()))
        }
        CreateOpt::Score(s) => a.kw("SCORE").val(s.clamp(0.0, 1.0)),
    }
}

fn render_field(a: Args, fs: &FieldSpec, on_json: bool) -> (Args, FieldKind) {
    let name = fs.name.as_str();
    let a = if on_json {
        a.kw(&format!("$.{name}")).kw("AS").kw(name)
    } else {
        a.kw(name)
    };
    let (a, kind) = match &fs.kind {
        FieldTypeSpec::Text {
            weight,
            nostem,
            phonetic,
        } => {
            let a = a
                .kw("TEXT")
                .when(weight.is_some(), |a| {
                    a.kw("WEIGHT").val(weight.unwrap() % 20)
                })
                .when(*nostem, |a| a.kw("NOSTEM"))
                .when(*phonetic, |a| a.kw("PHONETIC").kw("dm:en"));
            (a, FieldKind::Text)
        }
        FieldTypeSpec::Numeric => (a.kw("NUMERIC"), FieldKind::Numeric),
        FieldTypeSpec::Tag {
            separator,
            casesensitive,
        } => {
            let a = a
                .kw("TAG")
                .when(separator.is_some(), |a| {
                    a.kw("SEPARATOR")
                        .kw(&separator.as_ref().unwrap().ch().to_string())
                })
                .when(*casesensitive, |a| a.kw("CASESENSITIVE"));
            (a, FieldKind::Tag)
        }
        FieldTypeSpec::Geo => (a.kw("GEO"), FieldKind::Geo),
        FieldTypeSpec::Vector(v) => {
            let dim = v.dim.value();
            let mut attrs: Vec<(&str, String)> = vec![
                ("TYPE", data_str(&v.data).to_string()),
                ("DIM", dim.to_string()),
                ("DISTANCE_METRIC", metric_str(&v.metric).to_string()),
            ];
            if let Some(m) = v.m {
                attrs.push(("M", (u32::from(m) % 64).to_string()));
            }
            if let Some(ef) = v.ef_construction {
                attrs.push(("EF_CONSTRUCTION", (u32::from(ef) % 200).to_string()));
            }
            let mut a = a.kw("VECTOR").kw(algo_str(&v.algo)).val(attrs.len() * 2);
            for (k, val) in &attrs {
                a = a.kw(k).kw(val);
            }
            let data = match v.data {
                DataType::Float32 => VecType::Float32,
                DataType::Float64 => VecType::Float64,
            };
            (a, FieldKind::Vector { data, dim })
        }
    };
    let a = a
        .when(fs.sortable, |a| a.kw("SORTABLE"))
        .when(fs.noindex, |a| a.kw("NOINDEX"))
        .when(fs.index_missing, |a| a.kw("INDEXMISSING"));
    (a, kind)
}

fn render_search_opt(a: Args, opt: &SearchOpt, im: &IndexModel) -> Args {
    match opt {
        SearchOpt::Dialect(d) => a.kw("DIALECT").kw(d.as_str()),
        SearchOpt::Limit(o, n) => a.kw("LIMIT").kw(&o.as_str()).kw(&n.as_str()),
        SearchOpt::NoContent => a.kw("NOCONTENT"),
        SearchOpt::WithScores => a.kw("WITHSCORES"),
        SearchOpt::WithSortKeys => a.kw("WITHSORTKEYS"),
        SearchOpt::Verbatim => a.kw("VERBATIM"),
        SearchOpt::WithPayloads => a.kw("WITHPAYLOADS"),
        SearchOpt::InOrder => a.kw("INORDER"),
        SearchOpt::Slop(n) => a.kw("SLOP").kw(&n.as_str()),
        SearchOpt::Timeout(n) => a.kw("TIMEOUT").kw(&n.as_str()),
        SearchOpt::Scorer(s) => a.kw("SCORER").kw(s.as_str()),
        SearchOpt::InFields(r) => match pick_field_of(im, FieldKind::Text, *r) {
            Some(f) => a.kw("INFIELDS").kw("1").kw(&f),
            None => a,
        },
        SearchOpt::Return(r) => match pick_field(im, *r) {
            Some((f, _)) => a.kw("RETURN").kw("1").kw(&f),
            None => a,
        },
        SearchOpt::SortBy(r, ord) => match pick_field(im, *r) {
            Some((f, _)) => a.kw("SORTBY").kw(&f).kw(order_str(ord)),
            None => a,
        },
        SearchOpt::GeoFilter(r, spec) => match pick_field_of(im, FieldKind::Geo, *r) {
            Some(f) => a
                .kw("GEOFILTER")
                .kw(&f)
                .val(spec.lon)
                .val(spec.lat)
                .val(spec.radius.abs())
                .kw(spec.unit.as_str()),
            None => a,
        },
    }
}

fn render_agg_step(a: Args, step: &AggStep, im: &IndexModel) -> Args {
    match step {
        AggStep::Apply { expr, alias } => a
            .kw("APPLY")
            .raw(agg_expr(expr, im))
            .kw("AS")
            .kw(&format!("a{}", alias % 8)),
        AggStep::Filter(expr) => a.kw("FILTER").raw(agg_expr(expr, im)),
        AggStep::Limit(o, n) => a.kw("LIMIT").kw(&o.as_str()).kw(&n.as_str()),
        AggStep::SortBy(keys) => {
            let cols: Vec<(String, &str)> = keys
                .iter()
                .take(3)
                .filter_map(|(r, ord)| pick_field(im, *r).map(|(f, _)| (f, order_str(ord))))
                .collect();
            let a = a.kw("SORTBY").val(cols.len() * 2);
            cols.into_iter()
                .fold(a, |a, (f, ord)| a.kw(&format!("@{f}")).kw(ord))
        }
        AggStep::GroupBy { fields, reducers } => {
            let cols: Vec<String> = fields
                .iter()
                .take(3)
                .filter_map(|r| pick_field(im, *r).map(|(f, _)| f))
                .collect();
            let a = a.kw("GROUPBY").val(cols.len());
            let a = cols.into_iter().fold(a, |a, f| a.kw(&format!("@{f}")));
            reducers
                .iter()
                .take(4)
                .fold(a, |a, red| render_reducer(a, red, im))
        }
    }
}

fn render_reducer(a: Args, red: &Reducer, im: &IndexModel) -> Args {
    let num = |r: u16| numeric_arg(im, r);
    match red {
        Reducer::Count => a.kw("REDUCE").kw("COUNT").kw("0"),
        Reducer::Sum(r) => a.kw("REDUCE").kw("SUM").kw("1").kw(&num(*r)),
        Reducer::Avg(r) => a.kw("REDUCE").kw("AVG").kw("1").kw(&num(*r)),
        Reducer::Min(r) => a.kw("REDUCE").kw("MIN").kw("1").kw(&num(*r)),
        Reducer::Max(r) => a.kw("REDUCE").kw("MAX").kw("1").kw(&num(*r)),
        Reducer::ToList(r) => a.kw("REDUCE").kw("TOLIST").kw("1").kw(&num(*r)),
        Reducer::CountDistinct(r) => a.kw("REDUCE").kw("COUNT_DISTINCT").kw("1").kw(&num(*r)),
        Reducer::StdDev(r) => a.kw("REDUCE").kw("STDDEV").kw("1").kw(&num(*r)),
        Reducer::FirstValue(r) => a.kw("REDUCE").kw("FIRST_VALUE").kw("1").kw(&num(*r)),
        Reducer::Quantile(r, q) => a
            .kw("REDUCE")
            .kw("QUANTILE")
            .kw("2")
            .kw(&num(*r))
            .kw(q.as_str()),
    }
}

fn agg_expr(expr: &Expr, im: &IndexModel) -> Vec<u8> {
    let field = |r: u16| numeric_arg(im, r);
    match expr {
        Expr::FieldPlus(r, n) => format!("{} + {}", field(*r), n.as_str()),
        Expr::FieldTimesTwo(r) => format!("{} * 2", field(*r)),
        Expr::Sqrt(r) => format!("sqrt(abs({}))", field(*r)),
        Expr::UpperFormat(r) => format!("upper(format(\"%s\", {}))", field(*r)),
        Expr::FloorMod => "floor(1.5) % 3".to_string(),
        Expr::Div(r, n) => format!("({} - {}) / 2", field(*r), n.as_str()),
        Expr::Split => "split(\"a,b,c\", \",\")".to_string(),
        Expr::Greater(r, n) => format!("{} > {}", field(*r), n.as_str()),
        Expr::EqSelf(r) => format!("{} == {}", field(*r), field(*r)),
        Expr::Exists(r) => format!("exists({})", field(*r)),
        Expr::Garbage => "@@@".to_string(),
    }
    .into_bytes()
}

// ---- value helpers ---------------------------------------------------------

fn hash_value(kind: &FieldKind, v: &ValueSpec) -> Vec<u8> {
    match kind {
        FieldKind::Text => v.word.as_str().into(),
        FieldKind::Numeric => v.num.as_str().into_bytes(),
        FieldKind::Tag => {
            if v.multi_tag {
                format!("{},{}", v.word.as_str(), v.word.as_str()).into_bytes()
            } else {
                v.word.as_str().into()
            }
        }
        FieldKind::Geo => geo_point(&v.geo).into_bytes(),
        // Use the AST vector's own length (not the field's DIM) so mutations
        // naturally produce dimension mismatches; fall back to DIM when empty.
        FieldKind::Vector { data, dim } => {
            let n = if v.vector.is_empty() {
                *dim
            } else {
                v.vector.len()
            };
            blob(*data, n, &v.vector)
        }
    }
}

fn json_value(kind: &FieldKind) -> serde_json::Value {
    use serde_json::Value;
    match kind {
        FieldKind::Numeric => Value::from(1),
        FieldKind::Tag | FieldKind::Text => Value::from("hello"),
        FieldKind::Geo => Value::from("1.0,2.0"),
        FieldKind::Vector { dim, .. } => Value::Array(vec![Value::from(0.1); *dim]),
    }
}

fn blob(data: VecType, dim: usize, scalars: &[VecScalar]) -> Vec<u8> {
    let mut out = Vec::new();
    for i in 0..dim {
        let v = if scalars.is_empty() {
            0.0
        } else {
            scalars[i % scalars.len()].value()
        };
        match data {
            VecType::Float32 => out.extend_from_slice(&(v as f32).to_le_bytes()),
            VecType::Float64 => out.extend_from_slice(&v.to_le_bytes()),
        }
    }
    out
}

fn num_range(r: &NumRange) -> String {
    match r {
        NumRange::Closed(a, b) => format!("[{} {}]", a.as_str(), b.as_str()),
        NumRange::Open(a, b) => format!("[({} ({}]", a.as_str(), b.as_str()),
        NumRange::NegInfTo(b) => format!("[-inf {}]", b.as_str()),
        NumRange::ToPosInf(a) => format!("[{} +inf]", a.as_str()),
        NumRange::Bogus => "[a b]".to_string(),
    }
}

fn geo_spec(s: &GeoSpec) -> String {
    let (lon, lat) = geo_coords(s);
    format!("{lon} {lat} {} {}", s.radius.abs(), s.unit.as_str())
}

fn geo_point(s: &GeoSpec) -> String {
    let (lon, lat) = geo_coords(s);
    format!("{lon},{lat}")
}

fn geo_coords(s: &GeoSpec) -> (f64, f64) {
    if s.out_of_range {
        (200.0, 100.0)
    } else {
        (
            f64::from(s.lon).clamp(-180.0, 180.0),
            f64::from(s.lat).clamp(-85.0, 85.0),
        )
    }
}

fn render_params(params: Params) -> Vec<Vec<u8>> {
    if params.is_empty() {
        return Vec::new();
    }
    let mut out = vec![
        b"PARAMS".to_vec(),
        (params.len() * 2).to_string().into_bytes(),
    ];
    for (k, v) in params {
        out.push(k);
        out.push(v);
    }
    out
}

fn words(ws: &[Word]) -> Vec<Vec<u8>> {
    let list: Vec<Vec<u8>> = ws
        .iter()
        .take(4)
        .map(|w| w.as_str().as_bytes().to_vec())
        .collect();
    if list.is_empty() {
        vec![b"foo".to_vec()]
    } else {
        list
    }
}

fn pick_field(im: &IndexModel, r: u16) -> Option<(String, FieldKind)> {
    if im.fields.is_empty() {
        return None;
    }
    Some(im.fields[usize::from(r) % im.fields.len()].clone())
}

fn pick_field_of(im: &IndexModel, kind: FieldKind, r: u16) -> Option<String> {
    let fs = im.fields_of(kind);
    if fs.is_empty() {
        return None;
    }
    Some(fs[usize::from(r) % fs.len()].to_string())
}

/// Pick a field name for a typed query atom. Normally type-matched (valid, deep);
/// when `coerce` is set, pick from *any* field so the query type may mismatch the
/// field type — deliberately exercising error/assertion paths.
fn pick_typed(im: &IndexModel, kind: FieldKind, r: u16, coerce: bool) -> Option<String> {
    if coerce {
        pick_field(im, r).map(|(n, _)| n)
    } else {
        pick_field_of(im, kind, r)
    }
}

fn numeric_arg(im: &IndexModel, r: u16) -> String {
    match pick_field_of(im, FieldKind::Numeric, r) {
        Some(f) => format!("@{f}"),
        None => match pick_field(im, r) {
            Some((f, _)) => format!("@{f}"),
            None => "@score".to_string(),
        },
    }
}

fn join_sep(j: &Join) -> &'static [u8] {
    match j {
        Join::And => b" ",
        Join::Or => b"|",
        Join::Not => b" -",
        Join::Then => b" => ",
    }
}

fn order_str(o: &Order) -> &'static str {
    match o {
        Order::Asc => "ASC",
        Order::Desc => "DESC",
    }
}

fn algo_str(a: &Algo) -> &'static str {
    match a {
        Algo::Flat => "FLAT",
        Algo::Hnsw => "HNSW",
    }
}

fn data_str(d: &DataType) -> &'static str {
    match d {
        DataType::Float32 => "FLOAT32",
        DataType::Float64 => "FLOAT64",
    }
}

fn metric_str(m: &Metric) -> &'static str {
    match m {
        Metric::L2 => "L2",
        Metric::Ip => "IP",
        Metric::Cosine => "COSINE",
    }
}
