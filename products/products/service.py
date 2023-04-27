import logging
from typing import List

from nameko.events import event_handler
from nameko.rpc import rpc

from products.dependencies import Storage, Product


logger = logging.getLogger(__name__)


class ProductsService:
    """
    ProductsService

    This class provides RPC actions to get, list and create orders via entrypoints for the
    application. It also encapsulates business logic related to stock decrements whenever
    a product is sold through new orders.
    """

    name = 'products'

    storage = Storage()

    @rpc
    def get(self, product_id: str) -> Product:
        return self.storage.get(product_id)

    @rpc
    def list(self) -> List[Product]:
        return list(self.storage.list())

    @rpc
    def create(self, product: Product):
        self.storage.create(product)

    @event_handler('orders', 'order_created')
    def handle_order_created(self, payload: dict):
        for product in payload['order']['order_details']:
            self.storage.decrement_stock(
                product['product_id'], product['quantity'])
