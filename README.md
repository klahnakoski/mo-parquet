# mo-parquet

Read and write parquet files in pure Python, including nested object arrays

## Objective

Encode deep nested JSON and ensure schema expansion works over billions of JSON records


## Analysis

I have read the Dremel paper, and some other docs describing the Dremel paper, and I have come to the (proabably wrong) conclusion that definition levels are superfluous when encoding single properties (columns).  the paper admits the definition levels only encode missing values; specifically the definition level ofthe objects with missing values. If we are not interested in the missing values

missing values,, which means it Definition levels are an artifact of the whole-document deconstruction process; responsible for encoding document shape; the objects, not the property values.  In the paper you will notice the definition levels 

We assume all properties are of type repeated

The repetition level can encode any property that does exist. The definition level encodes the shape of the objects that are missing that property.  

When encoding `a.b` we must be able to encode the many ways it can 

|           json           | value |  rep  |  def  |
| ------------------------ | ----- | ----- | ----- |
|   null                   |  null |   0   |  -1   |
|   {}                     |  null |   0   |   0   |
|   {"a": {}}              |  null |   0   |   1   |
|   {"a": {"b": []}}       |  null |   0   |   2   |   
|   {"a": {"b": [1]}}      |   1   |   0   |   2   |
|   {"a": {"b": [1, 2]}}   |  1 2  |  0 2  |  2 2  |

When encoding a value, the definition level is always the maximum dimension (in this case 2). The definition is only for the nulls. Why is the null encoding mixed with the property? If it was the only property, it may make sense, but other properties will also encode these nulls. It would be more efficient to designate one column to encode the nulls, and drop the defintion levels in all the others 




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

