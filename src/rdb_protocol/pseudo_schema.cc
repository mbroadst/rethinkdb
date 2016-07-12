// Copyright 2010-2016 RethinkDB, all rights reserved.
#include "rdb_protocol/pseudo_schema.hpp"

namespace ql {
namespace pseudo {

const char *const schema_string = "SCHEMA";

void sanitize_schema(datum_t *schema) {
    r_sanity_check(schema != NULL);
    r_sanity_check(schema->is_ptype(schema_string));
}

} // namespace pseudo
} // namespace ql
