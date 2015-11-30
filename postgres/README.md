# Setup

1. Setup envars

    NOTE: These are credentials the test will use to connect to postgres.

    ```
    export POSTGRES_USER=postgres
    export POSTGRES_PASSWORD=admin
    ```

2. Install dependencies

    NOTE: If you're on a different platform and can't run the following command,
    there's a `psql` client inside the container as well so you can use that instead.
    More info regarding this below.

    ```
    sudo apt-get install postgresql-client-9.4
    ```

    And sqlx:

    ```
    go get github.com/jmoiron/sqlx
    ```

3. Spawn postgres container

    NOTE: This will download the container if not present
    and feel free to change the password.

    ```
    docker run -d \
        --name pgtest \
        -e "POSTGRES_USER=$POSTGRES_USER" \
        -e "POSTGRES_PASSWORD=$POSTGRES_PASSWORD" \
        -p 5432:5432 \
        postgres:9.4.5
    ```

4. Create database
    ```
    docker exec pgtest createdb -U $POSTGRES_USER cqrs_pg_test
    ```

5. Load the schema

    ```
    cd $GOPATH/src/github.com/andrewwebber/cqrs
    psql -U $POSTGRES_USER -d cqrs_pg_test -h localhost -p 5432 -W < ./postgres/schema.sql
    ```

    NOTE: If you're on a different platform and can't install postgres client
    system wide, you can copy over the schema inside the container and execute
    it from there using the bundled `psql` command inside, like:

    ```
    # copy schema to container
    docker cp ./postgres/schema.sql pgtest:/tmp

    # jump inside the container
    docker exec -it pgtest bash

    # load the schema from inside the container
    psql -U $POSTGRES_USER -d cqrs_pg_test -W < /tmp/schema.sql
    ```

# Run tests
```
cd postgres && go test
```
