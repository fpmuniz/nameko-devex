from nameko import config
from nameko.extensions import DependencyProvider
import redis

from products.exceptions import NotFound
from products.schemas import ProductSchema


REDIS_URI_KEY = 'REDIS_URI'

Product = dict


class StorageWrapper:
    """
    Product storage

    A very simple example of a custom Nameko dependency. Simplified
    implementation of products database based on Redis key value store.
    Handling the product ID increments or keeping sorted sets of product
    names for ordering the products is out of the scope of this example.

    """

    NotFound = NotFound

    def __init__(self, client: redis.StrictRedis):
        self.client: redis.StrictRedis = client

    def _format_key(self, product_id: str) -> str:
        return f'products:{product_id}'

    @classmethod
    def from_dict(cls, document: dict) -> Product:
        return ProductSchema().dump(document).data

    @classmethod
    def to_dict(cls, product: Product) -> dict:
        return ProductSchema(strict=True).load(product).data

    def get(self, product_id: str) -> dict:
        product = self.client.hgetall(self._format_key(product_id))
        if not product:
            raise NotFound(f'Product ID {product_id} does not exist')
        else:
            return self.from_dict(product)

    def list(self):
        keys = self.client.keys(self._format_key('*'))
        for key in keys:
            yield self.from_dict(self.client.hgetall(key))

    def create(self, product: Product):
        product = self.to_dict(product)
        self.client.hmset(
            self._format_key(product['id']),
            product
        )

    def decrement_stock(self, product_id: str, amount: int):
        return self.client.hincrby(
            self._format_key(product_id), 'in_stock', -amount
        )


class Storage(DependencyProvider):

    def setup(self):
        self.client = redis.StrictRedis.from_url(
            config.get(REDIS_URI_KEY),
            encoding='utf-8',
            decode_responses=True
        )

    def get_dependency(self, worker_ctx) -> StorageWrapper:
        return StorageWrapper(self.client)
