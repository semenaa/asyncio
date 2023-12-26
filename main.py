import asyncio
import aiohttp
from aioitertools.more_itertools import chunked
from migrate import Base, Session, SwapiPeople, engine

MAX_REQUESTS_CHUNK = 5


class PeopleIter:
    def __init__(self):
        self.counter = 1

    def __aiter__(self):
        return self

    async def __anext__(self):
        self.session = aiohttp.ClientSession()
        self.response = await self.session.get(f"https://swapi.py4e.com/api/people/{self.counter}")
        await self.session.close()
        if self.response.status == 404:
            raise StopAsyncIteration
        self.counter += 1
        self.json = await self.response.json()
        return self.json


async def insert_person(person):
    data = SwapiPeople(
        name=person['name'],
        height=person['height'],
        mass=person['mass'],
        hair_color=person['hair_color'],
        skin_color=person['skin_color'],
        eye_color=person['eye_color'],
        birth_year=person['birth_year'],
        gender=person['gender'],
        homeworld=person['homeworld'],
        films=', '.join(e for e in person['films']),
        species=', '.join(e for e in person['species']),
        vehicles=', '.join(e for e in person['vehicles']),
        starships=', '.join(e for e in person['starships'])
    )
    async with Session() as session:
        session.add(data)
        await session.commit()


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    people = PeopleIter()
    async for people_chunk in chunked(people, MAX_REQUESTS_CHUNK):
        for person_json in people_chunk:
            print(person_json)
            await insert_person(person_json)

    main_task = asyncio.current_task()
    insets_tasks = asyncio.all_tasks() - {main_task}
    await asyncio.gather(*insets_tasks)

    print("done")


if __name__ == "__main__":
    asyncio.run(main())
