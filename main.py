import asyncio
import aiohttp
from more_itertools import chunked

from migrate import Base, Session, SwapiPeople, engine

MAX_REQUESTS_CHUNK = 5


# class people_iter:
#     def __init__(self):
#         counter = 1
#
#     def __iter__(self):
#         return self
#
#     def __next__(self):
#         if type(self.current) is None:
#             raise StopIteration
#         self.counter += 1

def people_gen():
    counter = 0
    


async def insert_people(people_list_json):
    people_list = [SwapiPeople(json=person) for person in people_list_json]
    async with Session() as session:
        session.add_all(people_list)
        await session.commit()


async def get_people(people_id):
    session = aiohttp.ClientSession()
    response = await session.get(f"https://swapi.py4e.com/api/people/{people_id}")
    json_data = await response.json()
    await session.close()
    return json_data


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    for person_ids_chunk in chunked(people_gen(), MAX_REQUESTS_CHUNK):
        person_coros = [get_people(person_id) for person_id in person_ids_chunk]
        people = await asyncio.gather(*person_coros)
        insert_people_coro = insert_people(people)
        asyncio.create_task(insert_people_coro)

    main_task = asyncio.current_task()
    insets_tasks = asyncio.all_tasks() - {main_task}
    await asyncio.gather(*insets_tasks)

    print("done")


if __name__ == "__main__":
    start = datetime.datetime.now()
    asyncio.run(main())
    print(datetime.datetime.now() - start)
