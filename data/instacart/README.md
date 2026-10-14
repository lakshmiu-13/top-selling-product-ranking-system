# Instacart data setup

The raw Instacart files are not stored in this Git repository. After cloning,
download the Instacart Market Basket Analysis data from Kaggle using an account
that has access to the competition, and follow Kaggle's dataset terms. Extract
the archive under this directory, preserving the current directory names so
the required files are at these paths:

```text
data/instacart/
  products.csv/
    products.csv
  order_products__prior.csv/
    order_products__prior.csv
```

The producer requires `products.csv` and `order_products__prior.csv`. The
archive may also include `orders.csv` and `order_products__train.csv`; the
current replay producer does not use those files.

The large CSV and ZIP files are ignored by Git and excluded from the Docker
build context. Compose bind-mounts the local `data/` directory into the
pipeline container. Once the files are in place, start the application from
the repository root with `docker compose up --build`.

Each prior order-product row is replayed as one product occurrence, not as an
actual quantity or a current sale. See the repository README for replay speed,
event-count, and ranking interval settings.