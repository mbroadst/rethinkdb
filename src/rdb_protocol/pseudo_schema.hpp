// Copyright 2010-2016 RethinkDB, all rights reserved.
#ifndef RDB_PROTOCOL_PSEUDO_SCHEMA_HPP_
#define RDB_PROTOCOL_PSEUDO_SCHEMA_HPP_

#include "rdb_protocol/datum.hpp"

namespace ql {
class datum_t;

namespace pseudo {

extern const char *const schema_string;

void sanitize_schema(datum_t *geo);

} // namespace pseudo
} // namespace ql

#endif  // RDB_PROTOCOL_PSEUDO_SCHEMA_HPP_
