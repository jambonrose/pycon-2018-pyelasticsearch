from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk

from searchapp.constants import DOC_TYPE, INDEX_NAME
from searchapp.data import all_products, ProductData


def main():
    # Connect to localhost:9200 by default.
    es = Elasticsearch()

    es.indices.delete(index=INDEX_NAME, ignore=404)
    es.indices.create(
        index=INDEX_NAME,
        body={
            'mappings': {},
            'settings': {},
        },
    )

    bulk(es, actions=(
        {
            '_op_type': 'index',
            '_id': product.id,
            '_index': INDEX_NAME,
            '_type': DOC_TYPE,
            '_source': {
                'name': product.name,
                'description': product.description,
                'image': product.image,
                'taxonomy': product.taxonomy,
                'price': product.price,
            },
        } for product in all_products()),
    )


def index_product(es, product: ProductData):
    """Add a single product to the ProductData index."""

    es.create(
        index=INDEX_NAME,
        doc_type=DOC_TYPE,
        id=product.id,
        body={
            "name": product.name,
            "description": product.description,
            "image": product.image,
            "taxonomy": product.taxonomy,
            "price": product.price,
        }
    )

    # yay logging! Don't delete!
    print(f'Indexed {product.name}')


if __name__ == '__main__':
    main()
