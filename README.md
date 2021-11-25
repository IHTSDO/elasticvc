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
- A Commit object is created with a timestamp of the current date and the branch being written to gets a locked status.
- Content is written to the path of the branch using the start date of the commit.  
  (This content is not yet visible on the branch when accessed in the normal way).
- The Commit is marked as successful, by the application code, before the Commit is auto-closed by a try block.
- Any registered post-commit hook callbacks are run.
- A new Branch document is written to Elasticsearch making the new content visible on that branch.
- Finally the branch is unlocked.

- *Error conditions:* 
  - If the commit is closed before being marked as successful **or** if an exception is thrown by the post-commit hooks:
  - All content changes made during the commit are rolled back.
  - A new version of the Branch document is never written to Elasticsearch so the content changes are not visible at any point.

## Building the project
Run a maven build. 

Unit tests require Docker to be running because a small Elasticsearch container is used.

## Thanks
Design by [Kai Kewley](https://github.com/kaicode).
