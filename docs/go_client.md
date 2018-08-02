# redisearch
--
    import "github.com/RedisLabs/redisearch-go/redisearch"

Package redisearch provides a Go client for the RediSearch search engine.

For the full documentation of RediSearch, see
[https://oss.redislabs.com/redisearch](https://oss.redislabs.com/redisearch)


### Example Usage

```go

    import (
      "github.com/RedisLabs/redisearch-go/redisearch"
      "log"
      "fmt"
    )

    func ExampleClient() {
      // Create a client. By default a client is schemaless
      // unless a schema is provided when creating the index
      c := createClient("myIndex")

      // Create a schema
      sc := redisearch.NewSchema(redisearch.DefaultOptions).
        AddField(redisearch.NewTextField("body")).
        AddField(redisearch.NewTextFieldOptions("title", redisearch.TextFieldOptions{Weight: 5.0, Sortable: true})).
        AddField(redisearch.NewNumericField("date"))

      // Drop an existing index. If the index does not exist an error is returned
      c.Drop()

      // Create the index with the given schema
      if err := c.CreateIndex(sc); err != nil {
        log.Fatal(err)
      }

      // Create a document with an id and given score
      doc := redisearch.NewDocument("doc1", 1.0)
      doc.Set("title", "Hello world").
        Set("body", "foo bar").
        Set("date", time.Now().Unix())

      // Index the document. The API accepts multiple documents at a time
      if err := c.IndexOptions(redisearch.DefaultIndexingOptions, doc); err != nil {
        log.Fatal(err)
      }

      // Searching with limit and sorting
      docs, total, err := c.Search(redisearch.NewQuery("hello world").
        Limit(0, 2).
        SetReturnFields("title"))

      fmt.Println(docs[0].Id, docs[0].Properties["title"], total, err)
      // Output: doc1 Hello world 1 <nil>
    }

```

## Usage

```go
var DefaultIndexingOptions = IndexingOptions{
	Language: "",
	NoSave:   false,
	Replace:  false,
	Partial:  false,
}
```
DefaultIndexingOptions are the default options for document indexing

```go
var DefaultOptions = Options{
	NoSave:          false,
	NoFieldFlags:    false,
	NoFrequencies:   false,
	NoOffsetVectors: false,
	Stopwords:       nil,
}
```
DefaultOptions represents the default options

#### type Autocompleter

```go
type Autocompleter struct {
}
```

Autocompleter implements a redisearch auto-completer API

#### func  NewAutocompleter

```go
func NewAutocompleter(addr, name string) *Autocompleter
```
NewAutocompleter creates a new Autocompleter with the given host and key name

#### func (*Autocompleter) AddTerms

```go
func (a *Autocompleter) AddTerms(terms ...Suggestion) error
```
AddTerms pushes new term suggestions to the index

#### func (*Autocompleter) Delete

```go
func (a *Autocompleter) Delete() error
```
Delete deletes the Autocompleter key for this AC

#### func (*Autocompleter) Suggest

```go
func (a *Autocompleter) Suggest(prefix string, num int, fuzzy bool) ([]Suggestion, error)
```
Suggest gets completion suggestions from the Autocompleter dictionary to the
given prefix. If fuzzy is set, we also complete for prefixes that are in 1
Levenshten distance from the given prefix

#### type Client

```go
type Client struct {
}
```

Client is an interface to redisearch's redis commands

#### func  NewClient

```go
func NewClient(addr, name string) *Client
```
NewClient creates a new client connecting to the redis host, and using the given
name as key prefix. Addr can be a single host:port pair, or a comma separated
list of host:port,host:port... In the case of multiple hosts we create a
multi-pool and select connections at random

#### func (*Client) CreateIndex

```go
func (i *Client) CreateIndex(s *Schema) error
```
CreateIndex configues the index and creates it on redis

#### func (*Client) Drop

```go
func (i *Client) Drop() error
```
Drop the Currentl just flushes the DB - note that this will delete EVERYTHING on
the redis instance

#### func (*Client) Explain

```go
func (i *Client) Explain(q *Query) (string, error)
```
Explain Return a textual string explaining the query

#### func (*Client) Index

```go
func (i *Client) Index(docs ...Document) error
```
Index indexes a list of documents with the default options

#### func (*Client) IndexOptions

```go
func (i *Client) IndexOptions(opts IndexingOptions, docs ...Document) error
```
IndexOptions indexes multiple documents on the index, with optional Options
passed to options

#### func (*Client) Info

```go
func (i *Client) Info() (*IndexInfo, error)
```
Info - Get information about the index. This can also be used to check if the
index exists

#### func (*Client) Search

```go
func (i *Client) Search(q *Query) (docs []Document, total int, err error)
```
Search searches the index for the given query, and returns documents, the total
number of results, or an error if something went wrong

#### type ConnPool

```go
type ConnPool interface {
	Get() redis.Conn
}
```


#### type Document

```go
type Document struct {
	Id         string
	Score      float32
	Payload    []byte
	Properties map[string]interface{}
}
```

Document represents a single document to be indexed or returned from a query.
Besides a score and id, the Properties are completely arbitrary

#### func  NewDocument

```go
func NewDocument(id string, score float32) Document
```
NewDocument creates a document with the specific id and score

#### func (*Document) EstimateSize

```go
func (d *Document) EstimateSize() (sz int)
```

#### func (Document) Set

```go
func (d Document) Set(name string, value interface{}) Document
```
Set sets a property and its value in the document

#### func (*Document) SetPayload

```go
func (d *Document) SetPayload(payload []byte)
```
SetPayload Sets the document payload

#### type DocumentList

```go
type DocumentList []Document
```

DocumentList is used to sort documents by descending score

#### func (DocumentList) Len

```go
func (l DocumentList) Len() int
```

#### func (DocumentList) Less

```go
func (l DocumentList) Less(i, j int) bool
```

#### func (DocumentList) Sort

```go
func (l DocumentList) Sort()
```
Sort the DocumentList

#### func (DocumentList) Swap

```go
func (l DocumentList) Swap(i, j int)
```

#### type Field

```go
type Field struct {
	Name     string
	Type     FieldType
	Sortable bool
	Options  interface{}
}
```

Field represents a single field's Schema

#### func  NewNumericField

```go
func NewNumericField(name string) Field
```
NewNumericField creates a new numeric field with the given name

#### func  NewNumericFieldOptions

```go
func NewNumericFieldOptions(name string, options NumericFieldOptions) Field
```
NewNumericFieldOptions defines a numeric field with additional options

#### func  NewSortableNumericField

```go
func NewSortableNumericField(name string) Field
```
NewSortableNumericField creates a new numeric field with the given name and a
sortable flag

#### func  NewSortableTextField

```go
func NewSortableTextField(name string, weight float32) Field
```
NewSortableTextField creates a text field with the sortable flag set

#### func  NewTagField

```go
func NewTagField(name string) Field
```
NewTagField creates a new text field with default options (separator: ,)

#### func  NewTagFieldOptions

```go
func NewTagFieldOptions(name string, opts TagFieldOptions) Field
```
NewTagFieldOptions creates a new tag field with the given options

#### func  NewTextField

```go
func NewTextField(name string) Field
```
NewTextField creates a new text field with the given weight

#### func  NewTextFieldOptions

```go
func NewTextFieldOptions(name string, opts TextFieldOptions) Field
```
NewTextFieldOptions creates a new text field with given options
(weight/sortable)

#### type FieldType

```go
type FieldType int
```

FieldType is an enumeration of field/property types

```go
const (
	// TextField full-text field
	TextField FieldType = iota

	// NumericField numeric range field
	NumericField

	// GeoField geo-indexed point field
	GeoField

	// TagField is a field used for compact indexing of comma separated values
	TagField
)
```

#### type Flag

```go
type Flag uint64
```

Flag is a type for query flags

```go
const (
	// Treat the terms verbatim and do not perform expansion
	QueryVerbatim Flag = 0x1

	// Do not load any content from the documents, return just IDs
	QueryNoContent Flag = 0x2

	// Fetch document scores as well as IDs and fields
	QueryWithScores Flag = 0x4

	// The query terms must appear in order in the document
	QueryInOrder Flag = 0x08

	// Fetch document payloads as well as fields. See documentation for payloads on redisearch.io
	QueryWithPayloads Flag = 0x10

	DefaultOffset = 0
	DefaultNum    = 10
)
```
Query Flags

#### type HighlightOptions

```go
type HighlightOptions struct {
	Fields []string
	Tags   [2]string
}
```

HighlightOptions represents the options to higlight specific document fields.
See http://redisearch.io/Highlight/

#### type IndexInfo

```go
type IndexInfo struct {
	Schema               Schema
	Name                 string  `redis:"index_name"`
	DocCount             uint64  `redis:"num_docs"`
	RecordCount          uint64  `redis:"num_records"`
	TermCount            uint64  `redis:"num_terms"`
	MaxDocID             uint64  `redis:"max_doc_id"`
	InvertedIndexSizeMB  float64 `redis:"inverted_sz_mb"`
	OffsetVectorSizeMB   float64 `redis:"offset_vector_sz_mb"`
	DocTableSizeMB       float64 `redis:"doc_table_size_mb"`
	KeyTableSizeMB       float64 `redis:"key_table_size_mb"`
	RecordsPerDocAvg     float64 `redis:"records_per_doc_avg"`
	BytesPerRecordAvg    float64 `redis:"bytes_per_record_avg"`
	OffsetsPerTermAvg    float64 `redis:"offsets_per_term_avg"`
	OffsetBitsPerTermAvg float64 `redis:"offset_bits_per_record_avg"`
}
```

IndexInfo - Structure showing information about an existing index

#### type IndexingOptions

```go
type IndexingOptions struct {
	Language string
	NoSave   bool
	Replace  bool
	Partial  bool
}
```

IndexingOptions represent the options for indexing a single document

#### type MultiError

```go
type MultiError []error
```

MultiError Represents one or more errors

#### func  NewMultiError

```go
func NewMultiError(len int) MultiError
```
NewMultiError initializes a multierror with the given len, and all sub-errors
set to nil

#### func (MultiError) Error

```go
func (e MultiError) Error() string
```
Error returns a string representation of the error, in this case it just chains
all the sub errors if they are not nil

#### type MultiHostPool

```go
type MultiHostPool struct {
	sync.Mutex
}
```


#### func  NewMultiHostPool

```go
func NewMultiHostPool(hosts []string) *MultiHostPool
```

#### func (*MultiHostPool) Get

```go
func (p *MultiHostPool) Get() redis.Conn
```

#### type NumericFieldOptions

```go
type NumericFieldOptions struct {
	Sortable bool
	NoIndex  bool
}
```

NumericFieldOptions Options for numeric fields

#### type Operator

```go
type Operator string
```


```go
const (
	Eq Operator = "="

	Gt  Operator = ">"
	Gte Operator = ">="

	Lt  Operator = "<"
	Lte Operator = "<="

	Between          Operator = "BETWEEN"
	BetweenInclusive Operator = "BETWEEEN_EXCLUSIVE"
)
```

#### type Options

```go
type Options struct {

	// If set, we will not save the documents contents, just index them, for fetching ids only
	NoSave bool

	NoFieldFlags bool

	NoFrequencies bool

	NoOffsetVectors bool

	Stopwords []string
}
```

Options are flags passed to the the abstract Index call, which receives them as
interface{}, allowing for implementation specific options

#### type Paging

```go
type Paging struct {
	Offset int
	Num    int
}
```

Paging represents the offset paging of a search result

#### type Predicate

```go
type Predicate struct {
	Property string
	Operator Operator
	Value    []interface{}
}
```


#### func  Equals

```go
func Equals(property string, value interface{}) Predicate
```

#### func  GreaterThan

```go
func GreaterThan(property string, value interface{}) Predicate
```

#### func  GreaterThanEquals

```go
func GreaterThanEquals(property string, value interface{}) Predicate
```

#### func  InRange

```go
func InRange(property string, min, max interface{}, inclusive bool) Predicate
```

#### func  LessThan

```go
func LessThan(property string, value interface{}) Predicate
```

#### func  LessThanEquals

```go
func LessThanEquals(property string, value interface{}) Predicate
```

#### func  NewPredicate

```go
func NewPredicate(property string, operator Operator, values ...interface{}) Predicate
```

#### type Query

```go
type Query struct {
	Raw string

	Paging Paging
	Flags  Flag
	Slop   int

	Filters       []Predicate
	InKeys        []string
	ReturnFields  []string
	Language      string
	Expander      string
	Scorer        string
	Payload       []byte
	SortBy        *SortingKey
	HighlightOpts *HighlightOptions
	SummarizeOpts *SummaryOptions
}
```

Query is a single search query and all its parameters and predicates

#### func  NewQuery

```go
func NewQuery(raw string) *Query
```
NewQuery creates a new query for a given index with the given search term. For
currently the index parameter is ignored

#### func (*Query) Highlight

```go
func (q *Query) Highlight(fields []string, openTag, closeTag string) *Query
```
Highlight sets highighting on given fields. Highlighting marks all the query
terms with the given open and close tags (i.e. <b> and </b> for HTML)

#### func (*Query) Limit

```go
func (q *Query) Limit(offset, num int) *Query
```
Limit sets the paging offset and limit for the query

#### func (*Query) SetExpander

```go
func (q *Query) SetExpander(exp string) *Query
```
SetExpander sets a custom user query expander to be used

#### func (*Query) SetFlags

```go
func (q *Query) SetFlags(flags Flag) *Query
```
SetFlags sets the query's optional flags

#### func (*Query) SetInKeys

```go
func (q *Query) SetInKeys(keys ...string) *Query
```
SetInKeys sets the INKEYS argument of the query - limiting the search to a given
set of IDs

#### func (*Query) SetLanguage

```go
func (q *Query) SetLanguage(lang string) *Query
```
SetLanguage sets the query language, used by the stemmer to expand the query

#### func (*Query) SetPayload

```go
func (q *Query) SetPayload(payload []byte) *Query
```
SetPayload sets a binary payload to the query, that can be used by custom
scoring functions

#### func (*Query) SetReturnFields

```go
func (q *Query) SetReturnFields(fields ...string) *Query
```
SetReturnFields sets the fields that should be returned from each result. By
default we return everything

#### func (*Query) SetScorer

```go
func (q *Query) SetScorer(scorer string) *Query
```
SetScorer sets an alternative scoring function to be used. The only pre-compiled
supported one at the moment is DISMAX

#### func (*Query) SetSortBy

```go
func (q *Query) SetSortBy(field string, ascending bool) *Query
```
SetSortBy sets the sorting key for the query

#### func (*Query) Summarize

```go
func (q *Query) Summarize(fields ...string) *Query
```
Summarize sets summarization on the given list of fields. It will instruct the
engine to extract the most relevant snippets from the fields and return them as
the field content. This function works with the default values of the engine,
and only sets the fields. There is a function that accepts all options -
SummarizeOptions

#### func (*Query) SummarizeOptions

```go
func (q *Query) SummarizeOptions(opts SummaryOptions) *Query
```
SummarizeOptions sets summarization on the given list of fields. It will
instruct the engine to extract the most relevant snippets from the fields and
return them as the field content.

This function accepts advanced settings for snippet length, separators and
number of snippets

#### type Schema

```go
type Schema struct {
	Fields  []Field
	Options Options
}
```

Schema represents an index schema Schema, or how the index would treat documents
sent to it.

#### func  NewSchema

```go
func NewSchema(opts Options) *Schema
```
NewSchema creates a new Schema object

#### func (*Schema) AddField

```go
func (m *Schema) AddField(f Field) *Schema
```
AddField adds a field to the Schema object

#### type SingleHostPool

```go
type SingleHostPool struct {
	*redis.Pool
}
```


#### func  NewSingleHostPool

```go
func NewSingleHostPool(host string) *SingleHostPool
```

#### type SortingKey

```go
type SortingKey struct {
	Field     string
	Ascending bool
}
```

SortingKey represents the sorting option if the query needs to be sorted based
on a sortable fields and not a ranking function. See
http://redisearch.io/Sorting/

#### type Suggestion

```go
type Suggestion struct {
	Term    string
	Score   float64
	Payload string
}
```

Suggestion is a single suggestion being added or received from the Autocompleter

#### type SuggestionList

```go
type SuggestionList []Suggestion
```

SuggestionList is a sortable list of suggestions returned from an engine

#### func (SuggestionList) Len

```go
func (l SuggestionList) Len() int
```

#### func (SuggestionList) Less

```go
func (l SuggestionList) Less(i, j int) bool
```

#### func (SuggestionList) Sort

```go
func (l SuggestionList) Sort()
```
Sort the SuggestionList

#### func (SuggestionList) Swap

```go
func (l SuggestionList) Swap(i, j int)
```

#### type SummaryOptions

```go
type SummaryOptions struct {
	Fields       []string
	FragmentLen  int    // default 20
	NumFragments int    // default 3
	Separator    string // default "..."
}
```

SummaryOptions represents the configuration used to create field summaries. See
http://redisearch.io/Highlight/

#### type TagFieldOptions

```go
type TagFieldOptions struct {
	// Separator is the custom separator between tags. defaults to comma (,)
	Separator byte
	NoIndex   bool
}
```

TagFieldOptions options for indexing tag fields

#### type TextFieldOptions

```go
type TextFieldOptions struct {
	Weight   float32
	Sortable bool
	NoStem   bool
	NoIndex  bool
}
```

TextFieldOptions Options for text fields - weight and stemming enabled/disabled.
