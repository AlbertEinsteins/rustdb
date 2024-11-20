# rustdb
### A DBMS based on course cmu15445, but support for different storage engines, such as page-based or lsm-based

All aspects:
- the design of schema and index
- the storage engine
- the execution engine
- the transaction with txn_manager and lock_manager
- the index manager
- the top level design, mainly about parser、binder and scheduler
    - parser, includes lexer, grammar
    - binder, duty to check and convert a ast tree to a binder class
    - scheduler, simply, use a method to generate a execution plan, then optimize it with anything you can do

Current Step:
- Step into Storage engine
