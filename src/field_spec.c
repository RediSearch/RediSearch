#include "field_spec.h"
#include "spec.h"
#include "rmalloc.h"

///////////////////////////////////////////////////////////////////////////////////////////////

FieldSpec::FieldSpec(int idx, String field_name)
  : index{idx}
  , name{field_name}
  , types{0}
  , options{0}
  , ftId{(t_fieldId)-1}
  , ftWeight{1.0}
  , sortIdx{-1}
  , tagFlags{TAG_FIELD_DEFAULT_FLAGS}
  , tagSep{TAG_FIELD_DEFAULT_SEP}
{}

//---------------------------------------------------------------------------------------------

FieldSpec::FieldSpec(String field_name, IndexSpec *sp, ArgsCursor *ac, QueryError *status, bool isNew)
  : FieldSpec(sp->fields.size(), field_name)
{

  if (!parseFieldSpec(ac, status)) {
    throw Error("Parsing error");
  }

  if (IsFieldType(INDEXFLD_T_FULLTEXT) && IsIndexable()) {
    int textId = sp->CreateTextId();
    if (textId < 0) {
      status->SetError(QUERY_ELIMIT, "Too many TEXT fields in schema");
      throw Error("Too many TEXT fields in schema");
    }

    // If we need to store field flags and we have over 32 fields, we need to switch to wide
    // schema encoding
    if (textId >= SPEC_WIDEFIELD_THRESHOLD && (sp->flags & Index_StoreFieldFlags)) {
      if (isNew) {
        sp->flags |= Index_WideSchema;
      } else if ((sp->flags & Index_WideSchema) == 0) {
            status->SetError(QUERY_ELIMIT,
            "Cannot add more fields. Declare index with wide fields to allow adding "
            "unlimited fields");
        throw Error("Cannot add more fields. Declare index with wide fields to allow adding "
                    "unlimited fields");
      }
    }

    ftId = textId;
  }

  if (IsSortable()) {
    if (options & FieldSpec_Dynamic) {
      status->SetError(QUERY_EBADOPTION, "Cannot set dynamic field to sortable");
      throw Error("Cannot set dynamic field to sortable");
    }
    sortIdx = sp->sortables->Add(name, fieldTypeToValueType(types));
	if (sortIdx == -1) {
      status->SetError(QUERY_ELIMIT, "Too many SORTABLE fields in schema");
      throw Error("Too many SORTABLE fields in schema");
    }
  } else {
    sortIdx = -1;
  }

  if (IsPhonetics()) {
    sp->flags |= Index_HasPhonetic;
  }
}

//---------------------------------------------------------------------------------------------

void FieldSpec::SetSortable() {
  if (options & FieldSpec_Dynamic) throw Error("dynamic fields cannot be sortable");
  options |= FieldSpec_Sortable;
}

void FieldSpec::Initialize(FieldType type) {
  types |= type;
  if (IsFieldType(INDEXFLD_T_TAG)) {
    tagFlags = TAG_FIELD_DEFAULT_FLAGS;
    tagSep = TAG_FIELD_DEFAULT_SEP;
  }
}

//---------------------------------------------------------------------------------------------

// Parse a field definition from argv, at *offset. We advance offset as we progress.
// Returns 1 on successful parse, 0 otherwise

bool FieldSpec::parseFieldSpec(ArgsCursor *ac, QueryError *status) {
  if (ac->IsAtEnd()) {
    status->SetErrorFmt(QUERY_EPARSEARGS, "Field `%s` does not have a type", name);
    return false;
  }

  if (ac->AdvanceIfMatch(SPEC_TEXT_STR)) {
    Initialize(INDEXFLD_T_FULLTEXT);
    if (!parseTextField(ac, status)) {
      goto error;
    }
  } else if (ac->AdvanceIfMatch(NUMERIC_STR)) {
    Initialize(INDEXFLD_T_NUMERIC);
  } else if (ac->AdvanceIfMatch(GEO_STR)) {  // geo field
    Initialize(INDEXFLD_T_GEO);
  } else if (ac->AdvanceIfMatch(SPEC_TAG_STR)) {  // tag field
    Initialize(INDEXFLD_T_TAG);
    if (ac->AdvanceIfMatch(SPEC_SEPARATOR_STR)) {
      if (ac->IsAtEnd()) {
        status->SetError(QUERY_EPARSEARGS, SPEC_SEPARATOR_STR " requires an argument");
        goto error;
      }
      const char *sep = ac->GetStringNC(NULL);
      if (strlen(sep) != 1) {
        status->SetErrorFmt(QUERY_EPARSEARGS,
                               "Tag separator must be a single character. Got `%s`", sep);
        goto error;
      }
      tagSep = *sep;
    }
  } else {  // not numeric and not text - nothing more supported currently
    status->SetErrorFmt(QUERY_EPARSEARGS, "Invalid field type for field `%s`", name);
    goto error;
  }

  while (!ac->IsAtEnd()) {
    if (ac->AdvanceIfMatch(SPEC_SORTABLE_STR)) {
      SetSortable();
      continue;
    } else if (ac->AdvanceIfMatch(SPEC_NOINDEX_STR)) {
      options |= FieldSpec_NotIndexable;
      continue;
    } else {
      break;
    }
  }
  return true;

error:
  if (!status->HasError()) {
    status->SetErrorFmt(QUERY_EPARSEARGS, "Could not parse schema for field `%s`",
                           name);
  }
  return false;
}

//---------------------------------------------------------------------------------------------

bool FieldSpec::parseTextField(ArgsCursor *ac, QueryError *status) {
  int rc;
  // this is a text field
  // init default weight and type
  while (!ac->IsAtEnd()) {
    if (ac->AdvanceIfMatch(SPEC_NOSTEM_STR)) {
      options |= FieldSpec_NoStemming;
      continue;
    } else if (ac->AdvanceIfMatch(SPEC_WEIGHT_STR)) {
      double d;
      if ((rc = ac->GetDouble(&d, 0)) != AC_OK) {
        QERR_MKBADARGS_AC(status, "weight", rc);
        return false;
      }
      ftWeight = d;
      continue;
    } else if (ac->AdvanceIfMatch(SPEC_PHONETIC_STR)) {
      if (ac->IsAtEnd()) {
        status->SetError(QUERY_EPARSEARGS, SPEC_PHONETIC_STR " requires an argument");
        return false;
      }

      const char *matcher = ac->GetStringNC(NULL);
      // try and parse the matcher
      // currently we just make sure algorithm is double metaphone (dm)
      // and language is one of the following : English (en), French (fr), Portuguese (pt) and
      // Spanish (es)
      // in the future we will support more algorithms and more languages
      if (!checkPhoneticAlgorithmAndLang(matcher)) {
        status->SetError(QUERY_EINVAL,
        "Matcher Format: <2 chars algorithm>:<2 chars language>. Support algorithms: "
        "double metaphone (dm). Supported languages: English (en), French (fr), "
        "Portuguese (pt) and Spanish (es)");

        return false;
      }
      options |= FieldSpec_Phonetics;
      continue;
    } else {
      break;
    }
  }
  return true;
}

///////////////////////////////////////////////////////////////////////////////////////////////
