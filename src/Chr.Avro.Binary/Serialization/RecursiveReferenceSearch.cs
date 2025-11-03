namespace Chr.Avro.Serialization;

using System.Collections.Generic;
using System.Diagnostics;
using Chr.Avro.Abstract;

internal static class RecursiveReferenceSearch
{
    public static void Collect(Schema schema, ISet<Schema> result)
    {
        var context = new Context(result);
        Collect(schema, context);
    }

    private static void Collect(Schema schema, Context context)
    {
        if (!context.VisitedOnCurrentPath.Add(schema))
        {
            // We are re-visiting a type that is already on the current path, i.e. we have a recursion.
            // We will add every type that we have visited on the current path since last time we encountered schema to the results.
            for (var i = context.CurrentPath.Count - 1; i >= 0; i--)
            {
                var item = context.CurrentPath[i];
                context.Result.Add(item);
                if (Equals(item, schema))
                {
                    break;
                }
            }

            return;
        }

        context.CurrentPath.Add(schema);

        switch (schema)
        {
            case RecordSchema recordSchema:
                Collect(recordSchema, context);
                break;
            case UnionSchema unionSchema:
                Collect(unionSchema, context);
                break;
            case ArraySchema arraySchema:
                Collect(arraySchema, context);
                break;
            case MapSchema mapSchema:
                Collect(mapSchema, context);
                break;
        }

        Debug.Assert(context.VisitedOnCurrentPath.Contains(schema), "Schema not found on current path anymore");
        Debug.Assert(context.CurrentPath.Count > 0 && ReferenceEquals(context.CurrentPath[context.CurrentPath.Count - 1], schema), "List was not cleaned up properly");

        context.CurrentPath.RemoveAt(context.CurrentPath.Count - 1);
        context.VisitedOnCurrentPath.Remove(schema);
    }

    private static void Collect(RecordSchema schema, Context context)
    {
        foreach (var field in schema.Fields)
        {
            Collect(field.Type, context);
        }
    }

    private static void Collect(UnionSchema schema, Context context)
    {
        foreach (var innerSchema in schema.Schemas)
        {
            Collect(innerSchema, context);
        }
    }

    private static void Collect(ArraySchema schema, Context context)
    {
        Collect(schema.Item, context);
    }

    private static void Collect(MapSchema schema, Context context)
    {
        Collect(schema.Value, context);
    }

    private sealed class Context
    {
        public Context(ISet<Schema> result)
        {
            Result = result;
        }

        public ISet<Schema> Result { get; }

        public HashSet<Schema> VisitedOnCurrentPath { get; } = new HashSet<Schema>();

        public List<Schema> CurrentPath { get; } = new List<Schema>();
    }
}
