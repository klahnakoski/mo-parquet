# mo-parquet

Read and write parquet files in pure Python, including nested object arrays

## Objective

Encode deep nested JSON and ensure schema expansion works over billions of JSON records

## Code

Work is proceeding on [my fork of fastparquet](https://github.com/klahnakoski/fastparquet/blob/nested/fastparquet/json_writer.py)

## Analysis

I have read the Dremel paper, and some other docs describing the Dremel paper, and I have come to the conclusion that definition levels are superfluous when encoding properties. The Dremel paper admits the definition levels only encode missing values; specifically the definition level of the objects with missing values. If we are not interested in the missing values, then we should be able to avoid definition levels completely, while still having an accurate representation of existing property values.

### Simplifying Assumption

Understanding the Dremel encoding is easier if you first consider all properties REPEATED. With this, we can assume all nulls (and missing values) are an empty array (`[]`) and all values are a singleton array (`[value]`). These assumptions allow each leaf property to be represented by a `N`-dimensional array, where `N` is the nesting level of the property (plus additional dimensions if any are arrays of arrays). For each step on the path to the leaf value, there is a dimension that represents that step. 

Let us call multidimensional arrays "cubes".

Here is an example of the representative cube for for `a.b`

|           JSON           |      a.b 2d array     |
| ------------------------ | --------------------- |
|   null                   |          [[]]         |
|   {}                     |          [[]]         |
|   {"a": {}}              |          [[]]         |
|   {"a": {"b": []}}       |          [[]]         |   
|   {"a": {"b": [1]}}      |         [[1]]         |
|   {"a": {"b": [1, 2]}}   |        [[1, 2]]       |


For each `JSON` we extract the `a.b`; since most cases are missing that property, they show as empty 2d arrays. Notice we do not need a schema to build these arrays; they are a function of the property path we are extracting plus the JSON we are extracting from.  

### Repetition Number

The repetition number is a way of translating a plain series of values into these cubes; we know that all cubes are N dimensional, so the repetition number has nothing to say about where the values go; it says were the next (sub)cube begins: `0` is a whole new cube, `1` new first dimension of existing cube, `2` new second dimension of existing cube, etc.

When considering the REQUIRED and OPTIONAL properties, it will not change our interpretation of the repetition number. These restricted properties only define how the single-values appear in the original JSON, the repetition number is unchanged.

### Definition Number
 
The definition number is not simple; it encodes both nulls and values, and it must consider the nature (REQUIRED, OPTIONAL, REPEATED) of every column to calculate properly. For non-missing values the definition number is equal to the dimension minus the number of REQUIRED properties in the path; this means it is the same for all values of a given property.  

If we assume neither `a` nor `b` are REQUIRED, then the definition number is encoding the depth of the non-missing value OR the depth of null encountered. 

|           json           | value |  rep  |  def  |
| ------------------------ | ----- | ----- | ----- |
|   null                   |  null |   0   |  -1   |
|   {}                     |  null |   0   |   0   |
|   {"a": {}}              |  null |   0   |   1   |
|   {"a": {"b": []}}       |  null |   0   |   2   |   
|   {"a": {"b": [1]}}      |   1   |   0   |   2   |
|   {"a": {"b": [1, 2]}}   |  1 2  |  0 2  |  2 2  |




## Tests

Tests can be found https://github.com/Parquet/parquet-compatibility


## Notes 


Docs: https://github.com/apache/parquet-format

Good description: https://github.com/julienledem/redelm/wiki/The-striping-and-assembly-algorithms-from-the-Dremel-paper


### Low level encoding

The structures are encoded using [Thrift compaction protocol](https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md), specifically using [Thrift 110](https://issues.apache.org/jira/browse/THRIFT-110).  Here is a copy:

	message               => version-and-type seq-id method-name struct-encoding 
	version-and-type      => (6-bit version identifier) (2-bit type identifier)
	seq-id                => varint
	method-name           => varint (N-byte string)
	struct-encoding       => field_list stop
	field_list            => field field_list | field
	field                 => type-and-id value
	type-and-id           => field-id-delta type-header | 0 type-header zigzag-varint
	field-id-delta        => (4-bit offset from preceding field id, 1-15)
	type-header           => boolean-true | boolean-false | byte-type-header | i16-type-header | i32-type-header | i64-type-header | double-type-header | string-type-header | binary-type-header | list-type-header | set-type-header | map-type-header | struct-type-header
	value                 => boolean-true | boolean-false | byte | i16 | i32 | i64 | double | string | binary | list | set | map | struct
	stop                  => 0x0
	boolean-true          => 0x1
	boolean-false         => 0x2
	byte-type-header      => 0x3
	i16-type-header       => 0x4
	i32-type-header       => 0x5
	i64-type-header       => 0x6
	double-type-header    => 0x7
	binary-type-header    => 0x8
	string-type-header    => binary-type-header
	list-type-header      => 0x9
	set-type-header       => 0xA
	map-type-header       => 0xB
	struct-type-header    => 0xC
	byte                  => (1-byte value)
	i16                   => zigzag-varint
	i32                   => zigzag-varint
	i64                   => zigzag-varint
	double                => (8-byte double)
	binary                => varint(size) (bytes)
	string                => (utf-8 encoded)binary
	list                  => type-header varint list-body
	set                   => type-header varint list-body
	list-body             => value list-body | value
	map                   => (key)type-header (value)type-header varint key-value-pair-list
	key-value-pair-list   => key-value-pair key-value-pair-list | key-value-pair
	key-value-pair        => (key)value (value)value

