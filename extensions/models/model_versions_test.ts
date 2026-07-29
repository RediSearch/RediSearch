/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
/**
 * Checks the committed model instances against the types they name.
 *
 * Every instance under `models/` pins a `typeVersion`, and every model source
 * here declares a `version`. Nothing keeps them in step: editing a model means
 * remembering to bump its version and then to update each instance that names
 * it, and an instance left behind runs against a definition that no longer
 * exists in the tree.
 *
 * `swamp model validate` does not catch this — it checks the shape of an
 * instance and the expressions in it, not whether the version it names is the
 * one the repository ships. So it is checked here, where it costs nothing and
 * runs in CI without swamp installed.
 *
 * @module
 */
import { assertEquals } from "jsr:@std/assert@1";

/** Repository root, relative to the directory `deno test` runs in. */
const ROOT = new URL("../../", import.meta.url).pathname;

/** `type` and `version` as a model source declares them. */
function declaration(
  source: string,
): { type: string; version: string } | null {
  const type = source.match(/^\s{2}type: "([^"]+)",$/m);
  const version = source.match(/^\s{2}version: "([^"]+)",$/m);
  return type && version ? { type: type[1], version: version[1] } : null;
}

/** A field of a committed instance, which is flat YAML with quoted scalars. */
function field(yaml: string, name: string): string | null {
  const match = yaml.match(new RegExp(`^${name}: '?([^'\\n]+)'?$`, "m"));
  return match ? match[1].trim() : null;
}

/** Every model type this repository defines, keyed by type name. */
async function declaredVersions(): Promise<Map<string, string>> {
  const versions = new Map<string, string>();
  for await (const entry of Deno.readDir(`${ROOT}extensions/models`)) {
    if (!entry.isFile || !entry.name.endsWith(".ts")) continue;
    if (entry.name.endsWith("_test.ts")) continue;
    const declared = declaration(
      await Deno.readTextFile(`${ROOT}extensions/models/${entry.name}`),
    );
    if (declared) versions.set(declared.type, declared.version);
  }
  return versions;
}

/** Every committed instance, as the path it lives at plus what it names. */
async function instances(): Promise<
  Array<{ path: string; type: string; typeVersion: string }>
> {
  const found: Array<{ path: string; type: string; typeVersion: string }> = [];
  for await (const scope of Deno.readDir(`${ROOT}models`)) {
    if (!scope.isDirectory) continue;
    for await (const type of Deno.readDir(`${ROOT}models/${scope.name}`)) {
      if (!type.isDirectory) continue;
      const dir = `models/${scope.name}/${type.name}`;
      for await (const file of Deno.readDir(`${ROOT}${dir}`)) {
        if (!file.isFile || !file.name.endsWith(".yaml")) continue;
        const yaml = await Deno.readTextFile(`${ROOT}${dir}/${file.name}`);
        const named = field(yaml, "type");
        const version = field(yaml, "typeVersion");
        if (named && version) {
          found.push({
            path: `${dir}/${file.name}`,
            type: named,
            typeVersion: version,
          });
        }
      }
    }
  }
  return found;
}

Deno.test("every committed instance names a type this repository defines", async () => {
  const versions = await declaredVersions();
  const unknown = (await instances())
    .filter((instance) => !versions.has(instance.type))
    .map((instance) => `${instance.path} -> ${instance.type}`);

  assertEquals(unknown, []);
});

Deno.test("every committed instance is pinned to the current type version", async () => {
  const versions = await declaredVersions();
  // Reported as a list rather than one assertion per instance, so a version
  // bump that missed several says so once instead of one round trip each.
  const stale = (await instances())
    .filter((instance) => versions.has(instance.type))
    .filter((instance) => versions.get(instance.type) !== instance.typeVersion)
    .map((instance) =>
      `${instance.path}: pinned ${instance.typeVersion}, ` +
      `${instance.type} is ${versions.get(instance.type)}`
    );

  assertEquals(stale, []);
});

Deno.test("finds the instances it is meant to be checking", async () => {
  // The two checks above pass vacuously if the directory walk turns up
  // nothing — a renamed directory would make this suite silently stop testing
  // anything at all.
  const found = await instances();

  assertEquals(found.length > 0, true);
  assertEquals((await declaredVersions()).size > 0, true);
});
