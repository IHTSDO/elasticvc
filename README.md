# ElasticVC _Version Control for Elasticsearch_
Java library implementing version control for entities stored in Elasticsearch, with support for transactions and deep branching.

## How versioning works
### Class model

- `Entity` Abstract base class representing a version of an entity on a branch.
  - _path_: the branch path on which the entity exists. 
  - start: the point in time when the entity version started on that branch.
  - end: the point in time when the entity version ended on that branch (optional).
  - `Branch` Represents a branch of content
  - `DomainEntity` Base class for versioned content


A `Branch` document is created when a version control branch is first created and for each 
subsequent commit to that branch. All versioned content extends the class `DomainEntity` which has a start date and optionally 
an end date.

### Commit Lifecycle
TODO

### Branch Merging
TODO

## Building the project
Run a maven build. A Docker daemon must be installed running because the integration tests use a small container to host Elasticsearch.
