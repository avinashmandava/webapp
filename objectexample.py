from cassandra.cluster import Cluster
import logging
import time
from uuid import UUID
import random
from faker import Factory
#from pykafka import KafkaClient
import datetime

from cqlengine import columns
from cqlengine.models import Model
from cqlengine import connection
from cqlengine import management

class Config(object):
    cassandra_host = '127.0.0.1'
    keyspace = 'workflow'

class Generator(object):
    def generate_users(self,user_count):
        fake = Factory.create()
        users = []
        for i in range(1,user_count):
            user = User(user_id=fake.email(), user_name=fake.name(), user_password='welcome', valid=True)
            users.append(user)
        return users

    def generate_accounts(self,users,account_count):
        fake = Factory.create()
        accounts = []
        for user in users:
                for i in range(1,account_count):
                    account = Account(user_id=user.user_id, account_id=fake.company())
                    accounts.append(account)
        return accounts

    def generate_deals(self,accounts,deal_count):
        fake = Factory.create()
        deals = []
        for account in accounts:
            for i in range(1,deal_count):
                deal = Deal(user_id=account.user_id, account_id=account.account_id, deal_id=str(i))
                deals.append(deal)
        return deals

    def generate_tasks(self,deals,task_count):
        fake = Factory.create()
        tasks = []
        for deal in deals:
            for i in range(1,task_count):
                task = Task(user_id=deal.user_id, account_id=deal.account_id, deal_id=deal.deal_id, task_id=fake.bs(), description=fake.catch_phrase(), due_date=fake.date_time_between(start_date="now", end_date="+1y"), active=True, priority=fake.random_element(['H', 'M', 'L']))
                tasks.append(task)
        return tasks

class User(Model):
    user_id = columns.Text(partition_key=True)
    user_name = columns.Text()
    user_password = columns.Text()
    valid = columns.Boolean()

class Account(Model):
    user_id = columns.Text(partition_key=True)
    account_id = columns.Text(primary_key=true, clustering_order='ASC')

class Deal(Model):
    user_id = columns.Text(partition_key=True)
    account_id = columns.Text(partition_key=True)
    deal_id = columns.Text(primary_key=true, clustering_order='ASC')

class Task(Model):
    user_id = columns.Text(partition_key=True)
    account_id = columns.Text(partition_key=True)
    deal_id = columns.Text(partition_key=True)
    task_id = columns.Text(primary_key=true, clustering_order='ASC')
    description = columns.Text()
    due_date = columns.DateTime()
    active = columns.Boolean()
    priority = columns.Text()

class SimpleClient(object):
    def connect(self,node,ks_name):
        connection.setup([node],ks_name)

    def create_schema():
        management.delete_keyspace(Config.keyspace)
        management.create_keyspace("workflow", "SimpleStrategy", 1)
        management.sync_table(User)
        management.sync_table(Account)
        management.sync_table(Deal)
        management.sync_table(Task)

class WorkflowClient()
