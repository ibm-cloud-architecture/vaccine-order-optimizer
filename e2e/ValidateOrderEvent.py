from server.infrastructure.OrderConsumer import OrderConsumer
import logging

if __name__ == "__main__":
    print("-------- Start processing order event from Kafka ------- ")
    logging.basicConfig(level=1)
    orderConsumer = OrderConsumer.getInstance()
    orderConsumer.processEvents()
    # print(orderConsumer.getStore().getOrders())
