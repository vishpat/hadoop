#!/usr/bin/env python

from mrjob.job import MRJob
from mrjob.job import MRStep


class MRCustomerSpending(MRJob):

    def steps(self):
        steps = [
            MRStep(mapper=self.mapper_get_customer_spending,
                   reducer=self.reducer_aggregate_customer_spending),
            MRStep(
                mapper=self.mapper_total_spending,
                reducer=self.reducer_total_spending)
        ]
        return steps

    def mapper_get_customer_spending(self, _, line):
        customer, item, amount = line.split(',')
        yield "%04d" % int(customer), float(amount)

    def reducer_aggregate_customer_spending(self, customer, spendings):
        yield customer, "%.2f" % sum(spendings)

    def mapper_total_spending(self, customer, spendings):
        yield spendings, customer

    def reducer_total_spending(self, total_spending, customers):
        for customer in customers:
            yield customer, total_spending


if __name__ == "__main__":
    MRCustomerSpending.run()
