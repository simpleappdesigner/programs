import asyncio
from concurrent.futures import ThreadPoolExecutor
from requests_html import HTMLSession
import pandas as pd
import requests_cache
import time

def get_game_result(game):
    print(f"Processing game {game}")
    
    ses=HTMLSession()
    requests_cache.install_cache(expire_after=3600)
    r = ses.get('https://python.org/')
    print("start", game)

    final_list=[1,3,4,game]

    df = pd.DataFrame(final_list)
    df.to_csv(f"game_{game}.csv", index=False, header=False)

    return f"Finished processing game {game}"



if __name__ == '__main__':
    start_time=time.perf_counter()
    for num in range(10):
        get_game_result(num)
    t1=time.perf_counter() - start_time
    print(f"Execution time get_game_result(no threading): {t1}")


    start_time=time.perf_counter()
    with ThreadPoolExecutor(max_workers=8) as executor:
        for num in range(10):
            future = executor.submit( get_game_result,num)
            print(future.result())
        t2=time.perf_counter() - start_time
        print(f"Execution time get_game_result(with threading but waiting for each to complete): {t2}")
    

    start_time=time.perf_counter()
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures=[]
        for num in range(10):
            future = executor.submit( get_game_result,num)
            futures.append(future)
            #print(future.result())
        for future in futures:
            print(future.result())
        t3=time.perf_counter() - start_time
        print(f"Execution time get_game_result(with threading): {t3}")

    print(f"t1 vs t2 vs t3: {t1}:{t2}:{t3}")
