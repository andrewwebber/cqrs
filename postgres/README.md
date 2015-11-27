# Setup

1. Go to cqrs root
    ```
    cd $GOPATH/src/github.com/andrewwebber/cqrs
    ```

2. Create database for testing
    ```
    createdb cqrs_postgres_test
    ```

3. Load the schema
    ```
    psql -U postgres -d cqrs_postgres_test < ./postgres/schema.sql
    ```

# Run tests
```
cd postgres && go test
```
