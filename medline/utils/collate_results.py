# author; Ramji Chandrasekaran
# date: 19-Feb-2017
# collate documents per cluster and return a dataframe with 1 row per cluster

import pandas
import urllib


def collate_(input_df, base_url, num_clusters):
    """collate documents per cluster and return a dataframe with 1 row per cluster
       Parameters:
           input_df: dataframe containing uncollated cluster output"""

    cluster_urls = []
    for cluster_id in range(int(num_clusters)):
        search_terms = pandas.Series(input_df[input_df['cluster_id'] == cluster_id]['permalink']).tolist()
        full_url = base_url + urllib.parse.quote(" ".join(search_terms))
        cluster_urls.append(full_url)
        full_url = ""
    return pandas.DataFrame(cluster_urls, columns=['clickable content'])
