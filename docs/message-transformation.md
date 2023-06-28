# Message Transformation

The UCMessage class (`steps/data.py`) has methods to 'validate', 'transform',
and 'sanitise' the data.

These methods are translated from HTME (kotlin) into pyspark to maintain the same
output.  The names of the methods have been maintained, despite the 'validate' method
making changes to the data.

The unit tests from HTME have also been replicated and extended to ensure that the data
is transformed in the same way.

### UCMessage.validate
This method achieves several things:
- [Date Wrapping](#date-wrapping)
- [ID wrapping](#id-wrapping)
- [Setting `_lastModifiedDateTime`](#setting-lastmodifieddatetime)

#### Date wrapping
All date fields in the dbObject should look like this:
>`{"_lastModifiedDateTime": {"d_date": "YYYY-MM-DDTHH:MM:SS.FFFZ"}}`
>
> The date information should:
> - be wrapped with "d_date" as the key
> - the date information should be in UTC
> - the date information should be in the following format: "YYYY-MM-DDTHH:MM:SS.FFFZ"

#### ID wrapping
If the top level id field `_id` returns a JSON Primitive (not list or mapping)
then the value is wrapped

#### Setting _lastModifiedDateTime
The _lastModifiedDateTime is set with the following priority order:
  - $.message._lastModifiedDateTime (stays the same)
  - $.message._removedDateTime
  - $.message.createdDateTime
  - epoch (="1980-01-01T00:00:00.000Z" - as set by HTME)

### UCMessage.sanitise
This method takes the dbObject as a string, and does a 'dumb' find/replace for
unwanted characters:

| Find                | Replace            |
|---------------------|--------------------|
| `$`                 | `d_`               |
| `\u0000`            | < nothing >        |
| `_archivedDateTime` | `_removedDateTime` |
| `_archived`         | `_removed`         |


It also calls `UCMessage.sanitise_collection_specific`.  This is not yet fully
implemented, and raises an Exception when processing collections that require 
collection-specific sanitising.

### UCMessage.transform
This only applies to data.businessAudit messages because they have a different
schema when compared to the other collections. The information that would usually 
be at the top-level of normal collections' dbObject is hidden away in the 'context'
element of the audit message.

The method takes some information 
from the top level of the dbObject, and copies it into one of the dictionaries inside
the dbObject - the value of `$.message.dbObject.context`.

It then uses that 'context' object instead of the dbObject going forwards

