#!/bin/python
import json
import random
from faker import Faker
fake = Faker()


n = int(input('How many? '))
file = input('Output file: ')

data = []
for i in range(n):
    data.append({
        'name': fake.name(),
        'age': random.randint(3, 17),
        'salary': round(random.uniform(50, 5000), 2)
    })

with open(file, 'w') as f:
    f.write(json.dumps(data))
