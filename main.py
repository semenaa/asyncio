import asyncio
import aiohttp
from aioitertools.more_itertools import chunked
from migrate import Base, Session, SwapiPeople, engine

MAX_REQUESTS_CHUNK = 5


class PeopleIter:
    def __init__(self):
        self.counter = 81
    def __aiter__(self):
        return self

    async def __anext__(self):
        self.session = aiohttp.ClientSession()
        self.response = await self.session.get(f"https://swapi.py4e.com/api/people/{self.counter}")
        await self.session.close()
        if self.response.status == 404:
            raise StopAsyncIteration
        self.counter += 1
        return self.response


async def insert_people(people_list_json):
    people_list = [SwapiPeople(json=person) for person in people_list_json]
    async with Session() as session:
        session.add_all(people_list)
        await session.commit()


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    # for person_ids_chunk in chunked(range(1,100), MAX_REQUESTS_CHUNK):
    #     person_coros = [get_people(person_id) for person_id in person_ids_chunk]
    #     people = await asyncio.gather(*person_coros)
    #     insert_people_coro = insert_people(people)
    #     asyncio.create_task(insert_people_coro)

    people = PeopleIter()
    async for person in chunked(people, MAX_REQUESTS_CHUNK):
        print(person)

    main_task = asyncio.current_task()
    insets_tasks = asyncio.all_tasks() - {main_task}
    await asyncio.gather(*insets_tasks)

    print("done")


if __name__ == "__main__":
    asyncio.run(main())
