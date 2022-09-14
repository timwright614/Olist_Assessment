from webbrowser import get
import pandas as pd
import numpy as np
from olist.utils import haversine_distance
from olist.data import Olist
from functools import reduce

class Order:
    '''
    DataFrames containing all orders as index,
    and various properties of these orders as columns
    '''
    def __init__(self):
        # Assign an attribute ".data" to all new instances of Order
        self.data = Olist().get_data()

    def get_wait_time(self, is_delivered=True):
        """
        Returns a DataFrame with:
        [order_id, wait_time, expected_wait_time, delay_vs_expected, order_status]
        and filters out non-delivered orders unless specified
        """
        # Hint: Within this instance method, you have access to the instance of the class Order in the variable self, as well as all its attributes
        working_data = self.data['orders'].copy()

        if is_delivered:
            relevant_orders = working_data[working_data['order_status']=='delivered']
        else:
            relevant_orders = working_data

        datecols = ['order_purchase_timestamp','order_approved_at','order_delivered_carrier_date','order_delivered_customer_date','order_estimated_delivery_date']

        for datecol in datecols:
            relevant_orders[datecol] = pd.to_datetime(relevant_orders[datecol])

        wait_time =  relevant_orders['order_delivered_customer_date'] - relevant_orders['order_purchase_timestamp']
        expected_wait_time = relevant_orders['order_estimated_delivery_date'] - relevant_orders['order_purchase_timestamp']
        delay_vs_expected = wait_time - expected_wait_time

        get_wait_time = pd.concat([relevant_orders['order_id'],relevant_orders['order_status']],axis =1)
        get_wait_time['wait_time'] = wait_time
        get_wait_time['expected_wait_time'] = expected_wait_time
        get_wait_time['delay_vs_expected'] = delay_vs_expected

        get_wait_time['expected_wait_time'] = get_wait_time['expected_wait_time'].apply(lambda x: (x.total_seconds()//(3600*24)) + (x.total_seconds()%(3600*24)//3600)/24)
        get_wait_time['wait_time'] = get_wait_time['wait_time'].apply(lambda x: (x.total_seconds()//(3600*24)) + (x.total_seconds()%(3600*24)//3600)/24)
        get_wait_time['delay_vs_expected'] = get_wait_time ['delay_vs_expected'].apply(lambda x: (x.total_seconds()//(3600*24)) + (x.total_seconds()%(3600*24)//3600)/24)

        return get_wait_time

    def get_review_score(self):
        """
        Returns a DataFrame with:
        order_id, dim_is_five_star, dim_is_one_star, review_score
        """
        working_reviews = self.data['order_reviews'][['order_id','review_score']].copy()
        working_reviews['dim_is_five_star'] = np.where(working_reviews['review_score'] == 5,1,0)
        working_reviews['dim_is_one_star'] = np.where(working_reviews['review_score'] == 1,1,0)

        return working_reviews

    def get_number_products(self):
        """
        Returns a DataFrame with:
        order_id, number_of_products
        """
        working_products = self.data['order_items'][['order_id','product_id']].copy()

        product_count = working_products.groupby('order_id')['product_id'].count().to_frame()

        product_count.columns = ['number_of_products']

        return product_count.reset_index()


    def get_number_sellers(self):
        """
        Returns a DataFrame with:
        order_id, number_of_sellers
        """
        working_sellers = self.data['order_items'][['order_id','seller_id']].copy()

        seller_count = working_sellers.groupby('order_id')['seller_id'].nunique().to_frame()

        seller_count.columns = ['number_of_sellers']

        return seller_count.reset_index()

    def get_price_and_freight(self):
        """
        Returns a DataFrame with:
        order_id, price, freight_value
        """
        working_price_freight = self.data['order_items'][['order_id','price','freight_value']].copy()

        return working_price_freight.groupby('order_id').sum().reset_index()

    # Optional
    def get_distance_seller_customer(self):
        """
        Returns a DataFrame with:
        order_id, distance_seller_customer
        """
        pass  # YOUR CODE HERE

    def get_training_data(self,
                          is_delivered=True,
                          with_distance_seller_customer=False):
        """
        Returns a clean DataFrame (without NaN), with the all following columns:
        ['order_id', 'wait_time', 'expected_wait_time', 'delay_vs_expected',
        'order_status', 'dim_is_five_star', 'dim_is_one_star', 'review_score',
        'number_of_products', 'number_of_sellers', 'price', 'freight_value',
        'distance_seller_customer']
        """
        # Hint: make sure to re-use your instance methods defined above
        df_list = [self.get_wait_time(is_delivered=is_delivered),
                self.get_review_score(),
                self.get_number_products(),
                self.get_number_sellers(),
                self.get_price_and_freight()]

        df_merged = reduce(lambda  left,right: pd.merge(left,right,on=['order_id'],
                                            how='outer'), df_list)

        return df_merged.dropna()
