// Copyright 2010-2015 RethinkDB, all rights reserved.
#include "rdb_protocol/op.hpp"
#include "rdb_protocol/term.hpp"
#include "rdb_protocol/terms/terms.hpp"
#include "rdb_protocol/pseudo_schema.hpp"

#include "rapidjson/document.h"
#include "rapidjson/error/en.h"
#include "rapidjson/rapidjson.h"
#include "rapidjson/stringbuffer.h"
#include "rapidjson/writer.h"
#include "rapidjson/schema.h"

namespace ql {

class schema_term_t : public op_term_t {
public:
    schema_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env, term, argspec_t(1)) { }

    scoped_ptr_t<val_t> eval_impl(scope_env_t *env, args_t *args, eval_flags_t) const {
        const datum_t datum = args->arg(env, 0)->as_datum();
        if (datum.get_type() == datum_t::R_STR) {
          // NOTE: this is and `r.json` should share an implementation, as this is
          //       is ripped completely from there.
          const datum_string_t data = args->arg(env, 0)->as_str();

          // Copy the string into a null-terminated c-string that we can write
          // to, so we can use RapidJSON in-situ parsing (and at least avoid
          // some additional copying).
          std::vector<char> str_buf(data.size() + 1);
          memcpy(str_buf.data(), data.data(), data.size());
          for (size_t i = 0; i < data.size(); ++i) {
              rcheck(str_buf[i] != '\0', base_exc_t::LOGIC,
                      "Encountered unescaped null byte in JSON string.");
          }
          str_buf[data.size()] = '\0';

          rapidjson::Document json;
          // Note: Insitu will cause some parts of `json` to directly point into
          // `str_buf`. `str_buf`'s life time must be at least as long as `json`'s.
          json.ParseInsitu(str_buf.data());

          rcheck(!json.HasParseError(), base_exc_t::LOGIC,
                  strprintf("Failed to parse \"%s\" as JSON: %s",
                      (data.size() > 40
                      ? (data.to_std().substr(0, 37) + "...").c_str()
                      : data.to_std().c_str()),
                      rapidjson::GetParseError_En(json.GetParseError())));

          datum_object_builder_t result(
            to_datum(json, env->env->limits(), env->env->reql_version()));
          bool dup = result.add(datum_t::reql_type_string, datum_t(pseudo::schema_string));
          rcheck(!dup, base_exc_t::LOGIC, "Schema object already had a "
                                            "$reql_type$ field.");

          return new_val(std::move(result).to_datum());
        }

        // otherwise assume R_OBJECT and store the object inline, adding a $reql_type$ field
        datum_object_builder_t result(datum);
        bool dup = result.add(datum_t::reql_type_string, datum_t(pseudo::schema_string));
        rcheck(!dup, base_exc_t::LOGIC, "Schema object already had a "
                                          "$reql_type$ field.");

        return new_val(std::move(result).to_datum());
    }

    virtual const char *name() const { return "schema"; }
};

class validate_term_t : public op_term_t {
public:
    validate_term_t(compile_env_t *env, const raw_term_t &term)
        : op_term_t(env, term, argspec_t(2)) { }

private:
    // Validation is done by a single RapidJSON Schema Validator and presently
    // can only catch a single error at a time - restrict to a single server.
    deterministic_t is_deterministic() const {
        return worst_determinism(
          op_term_t::is_deterministic(), deterministic_t::always);
    }

    scoped_ptr_t<val_t> eval_impl(scope_env_t *env, args_t *args, eval_flags_t) const {
        scoped_ptr_t<val_t> s1_arg = args->arg(env, 0);
        scoped_ptr_t<val_t> s2_arg = args->arg(env, 1);

        datum_t data_datum;
        datum_t schema_datum;
        if (s1_arg->as_datum().is_ptype(pseudo::schema_string)) {
            data_datum = s2_arg->as_datum();
            schema_datum = s1_arg->as_ptype(pseudo::schema_string);
        } else {
            data_datum = s1_arg->as_datum();
            schema_datum = s2_arg->as_ptype(pseudo::schema_string);
        }

        rapidjson::Document data_doc;
        rapidjson::Value json_value = data_datum.as_json(&data_doc.GetAllocator());
        data_doc.CopyFrom(json_value, data_doc.GetAllocator());

        rapidjson::Document schema_doc;
        rapidjson::Value schema_value = schema_datum.as_json(&schema_doc.GetAllocator());
        schema_doc.CopyFrom(schema_value, schema_doc.GetAllocator());

        rapidjson::SchemaDocument schema(schema_doc);
        rapidjson::SchemaValidator validator(schema);
        if (!data_doc.Accept(validator)) {
          // we will want to do more with the result here
          return new_val_bool(false);
        }

        return new_val_bool(true);
    }

    virtual const char *name() const { return "validate"; }
};

counted_t<term_t> make_schema_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<schema_term_t>(env, term);
}

counted_t<term_t> make_validate_term(
        compile_env_t *env, const raw_term_t &term) {
    return make_counted<validate_term_t>(env, term);
}

} // namespace ql
