using System;
using System.Collections.Generic;
using Chr.Avro.Abstract;
using Xunit;

namespace Chr.Avro.Serialization.Tests;

public class RecursiveReferenceSearchTests
{
    public static readonly TheoryData<Schema> NonRecursiveSchemas = new()
    {
        // Primitive
        { new BooleanSchema() },
        { new BytesSchema() },
        { new DoubleSchema() },
        { new FloatSchema() },
        { new IntSchema() },
        { new LongSchema() },
        { new NullSchema() },
        { new StringSchema() },

        // Complex
        { new ArraySchema(new BooleanSchema()) },
        { new ArraySchema(new ArraySchema(new StringSchema())) },
        { new MapSchema(new DoubleSchema()) },
        { new MapSchema(new ArraySchema(new MapSchema(new IntSchema()))) },
        { new UnionSchema(Array.Empty<Schema>()) },
        { new UnionSchema(new Schema[] { new IntSchema() }) },
        { new UnionSchema(new Schema[] { new IntSchema(), new StringSchema() }) },
        { new UnionSchema(new Schema[] { new IntSchema(), new StringSchema(), new ArraySchema(new FloatSchema()) }) },

        // Named
        { new EnumSchema("MyEnum", new string[] { "hello", "world" }) },
        { new FixedSchema("fixed", 123) },
    };


    [Theory]
    [MemberData(nameof(NonRecursiveSchemas), MemberType = typeof(RecursiveReferenceSearchTests))]
    public void IdentifyNonRecursiveSchemas(Schema schema)
    {
        var set = new HashSet<Schema>();
        RecursiveReferenceSearch.Collect(schema, set);
        Assert.Empty(set);
    }

    [Fact]
    public void IdentifiesImmediateRecursion()
    {
        var record = new RecordSchema("record");
        record.Fields.Add(new RecordField("primitive", new IntSchema()));
        record.Fields.Add(new RecordField("child", record));

        var set = new HashSet<Schema>();
        RecursiveReferenceSearch.Collect(record, set);
        Assert.Single(set);
        Assert.Contains(record, set);
    }

    [Fact]
    public void IdentifiesImmediateRecursionWithTwoChildren()
    {
        var record = new RecordSchema("record");
        record.Fields.Add(new RecordField("primitive", new IntSchema()));
        record.Fields.Add(new RecordField("child", record));
        record.Fields.Add(new RecordField("child2", record));

        var set = new HashSet<Schema>();
        RecursiveReferenceSearch.Collect(record, set);
        Assert.Single(set);
        Assert.Contains(record, set);
    }


    [Fact]
    public void IdentifiesNestedRecursion()
    {
        var recursiveRecord = new RecordSchema("a");
        var unionSchema = new UnionSchema(new Schema[] { recursiveRecord, new NullSchema() });
        var arraySchema = new ArraySchema(unionSchema);
        recursiveRecord.Fields.Add(new RecordField("b", new IntSchema()));
        var intermediate = new RecordSchema("c");
        intermediate.Fields.Add(new RecordField("d", arraySchema));
        recursiveRecord.Fields.Add(new RecordField("f", intermediate));

        var topLevelRecord = new RecordSchema("g");
        topLevelRecord.Fields.Add(new RecordField("h", recursiveRecord));

        var set = new HashSet<Schema>();
        RecursiveReferenceSearch.Collect(topLevelRecord, set);
        Assert.Equal(4, set.Count);
        Assert.Contains(recursiveRecord, set);
        Assert.Contains(intermediate, set);
        Assert.Contains(unionSchema, set);
        Assert.Contains(arraySchema, set);
    }
}
