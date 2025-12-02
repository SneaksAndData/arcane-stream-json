## JSON Stream Plugin for Arcane
This repository contains implementation of a JSON-Iceberg streaming plugin for Arcane. Use this app to livestream Json files to an Iceberg table, backed by [Trino](https://github.com/trinodb/trino) as a streaming batch merge consumer and [Lakekeeper](https://github.com/lakekeeper/lakekeeper) as a data catalog.

### Quickstart

This source continuously ingests files with `multiline-JSON` content into a target Iceberg table. In order to configure the stream, you must provide the following:
- *Desired* AVRO schema for the source. Note that this schema should conform with JSON created *after* JSON pointers and array explode have been applied. All fields in the schema must be defined as `nullable`. You can use this [handy tool](https://jonathanfiss.github.io/convert-json-to-avro-schema/) to generate the schema.
- Source S3 path
- JSON pointer expression, if desired data is a subset of a source json. For example, given
```json
{
  "colA": "a",
  "colB": {
    "colC": "c",
    "propA": 1,
    "propB": "ABC"
  }
}
```
and `jsonPointerExpression` set to `/colB`, source will be transformed to:
```json
{
  "colC": "c",
  "propA": 1,
  "propB": "ABC"
}
```
- JSON pointers for array explode, if any. For example, given
```json
{
  "colA": "a",
  "colB": [{
    "colC": "c1",
    "propA": 1,
    "propB": "ABC1"
  },{
    "colC": "c2",
    "propA": 2,
    "propB": "ABC2"
  }]
}
```
and `jsonArrayPointers` set to `"/colB": {}`, source will be transformed to:
```json
{"colC": "c1", "propA": 1, "propB": "ABC1"}
{"colC": "c2", "propA": 2, "propB": "ABC2"}
```
emitting 2 rows from 1 source file entry.

### Development

Project uses `Scala 3.6.1` and tested on JDK 23.
