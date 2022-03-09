#install.packages("academictwitteR")

library(tidyverse)
library(academictwitteR)

#set_bearer()
get_bearer()

query <- "AAPL"

qry_string <- paste0("($",query,") lang:en")

nested_data <- get_all_tweets(
  query = c("#PoliSciBakes"),
  start_tweets = "2020-01-14T05:00:00Z",
  end_tweets = "2021-01-14T05:00:00Z",
  data_path = "data",
  n = 1000000)

unnest_tweet_data <- function(qry,dta) {
  output_list <- list()
  
  output_list[["primary_tweet_data"]]  <- dta %>% 
    select(where(negate(is.list))) %>% 
    as_tibble() %>% 
    mutate(query = qry)
  
  
  
  if("entities" %in% names(dta)) {
    entities <- dta%>% select(entities) %>% 
      map_if(is.data.frame, list) %>%
      as_tibble() %>% 
      unnest(entities,names_sep = ".") %>% 
      bind_cols(dta%>% select(id))
    
    if("entities.mentions" %in% names(entities)) {
      output_list[["mentions"]] <- entities %>% 
        select(entities.mentions,id) %>% 
        unnest(entities.mentions,names_sep=".") 
    }
    
    if("entities.urls" %in% names(entities)) {
      urls <- entities %>% select(entities.urls,id) %>% 
        unnest(entities.urls,names_sep=".") 
      
      if("entities.urls.images" %in% names(urls)) {
        output_list[["url_images"]] <- urls %>% 
          select(entities.urls.images,id) %>% 
          map_if(is.data.frame, list) %>%
          as_tibble() %>% unnest(entities.urls.images)
      }
      
      output_list[["urls"]] <- urls %>% 
        select(-contains("entities.urls.images"))
    }
    
    if("entities.cashtags" %in% names(entities)) {
      output_list[["cashtags"]] <- entities %>% 
        select(entities.cashtags,id) %>% 
        unnest(entities.cashtags,names_sep=".") 
    }
    
    if("entities.annotations" %in% names(entities)) {
      output_list[["entity_annotations"]] <- entities %>% 
        select(entities.annotations,id) %>% 
        unnest(entities.annotations,names_sep=".") 
    }
    
    if("entities.hashtags" %in% names(entities)) {
      output_list[["hashtags"]] <- entities %>% 
        select(entities.hashtags,id) %>% 
        unnest(entities.hashtags,names_sep=".") 
    }
  }
  
  if("referenced_tweets" %in% names(dta)) {
    output_list[["ref_tweets"]]  <- dta %>% select(referenced_tweets,id) %>% 
      map_if(is.data.frame, list) %>%
      as_tibble() %>%
      unnest(referenced_tweets, names_sep = ".") 
  }
  
  if("public_metrics" %in% names(dta)) {
    output_list[["public_metrics"]]  <- dta%>% select(public_metrics) %>% 
      map_if(is.data.frame, list) %>%
      as_tibble() %>% 
      unnest(public_metrics,names_sep = ".") %>% 
      bind_cols(dta%>% select(id))
  }
  
  if("attachments" %in% names(dta)) {
    attachments <- dta%>% select(attachments) %>% 
      map_if(is.data.frame, list) %>%
      as_tibble() %>% 
      unnest(attachments,names_sep = ".") %>% 
      bind_cols(dta%>% select(id))
    
    if("attachments.media_keys" %in% names(attachments)) {
      output_list[["media_keys"]]  <- attachments %>% 
        select(attachments.media_keys,id) %>% 
        unnest(attachments.media_keys) 
    }
    if("attachments.poll_ids" %in% names(attachments)) {
      output_list[["poll_ids"]] <- attachments %>% 
        select(attachments.poll_ids,id) %>% 
        unnest(attachments.poll_ids) 
    } 
  }
  
  if("context_annotations" %in% names(dta)) {
    context_annotations <- dta %>% select(context_annotations,id) %>% 
      map_if(is.data.frame, list) %>%
      as_tibble() %>% unnest(context_annotations) 
    
    
    output_list[["context_annotations"]]  <- context_annotations %>% 
      select(domain) %>% 
      map_if(is.data.frame, list) %>%
      as_tibble() %>% 
      unnest(domain, names_sep = ".") %>% 
      bind_cols(
        context_annotations %>% select(entity) %>% 
          map_if(is.data.frame, list) %>%
          as_tibble() %>% 
          unnest(entity, names_sep = ".")
      ) %>% 
      bind_cols(context_annotations %>% select(id))
  }
  
  if("geo" %in% names(dta)) {
    geo <- dta%>% select(id) %>% 
      bind_cols(
        dta %>% select(geo) %>% 
          map_if(is.data.frame, list) %>%
          as_tibble() %>% 
          unnest(geo,names_sep = ".") 
      )
    
    geo_temp <- geo %>% select(-contains("geo.coordinates")) 
    
    
    if("geo.coordinates" %in% names(geo)) {
      geo_coord <- geo %>% select(geo.coordinates)%>% 
        map_if(is.data.frame, list) %>%
        as_tibble() %>% 
        unnest(geo.coordinates,names_sep=".")  %>% 
        mutate(coordinates = as.character(geo.coordinates.coordinates)) %>% 
        select(-geo.coordinates.coordinates) 
      
      geo_temp <- bind_cols(geo_temp,geo_coord)
    }
    
    output_list[["geo"]] <- geo_temp
  }
  
  return(output_list)
  
}

unnested_data_list <- unnest_tweet_data(qry=query,dta=nested_data)

#make individual dfs
tweet_data <- unnested_data_list$primary_tweet_data
mentions <- unnested_data_list$mentions
url_images <- unnested_data_list$url_images
urls <- unnested_data_list$urls
cashtags <- unnested_data_list$cashtags
entity_annotations <- unnested_data_list$entity_annotations
hashtags <- unnested_data_list$hashtags
ref_tweets <- unnested_data_list$ref_tweets
public_metrics <- unnested_data_list$public_metrics
media_keys <- unnested_data_list$media_keys
poll_ids <- unnested_data_list$poll_ids
geo <- unnested_data_list$geo

df <- merge(tweet_data, mentions, by = "id", all = TRUE)
df <- merge(df, url_images, by = "id", all = TRUE)
df <- merge(df, urls, by = "id", all = TRUE)
df <- merge(df, cashtags, by = "id", all = TRUE)
df <- merge(df, entity_annotations, by = "id", all = TRUE)
df <- merge(df, hashtags, by = "id", all = TRUE)
df <- merge(df, ref_tweets, by = "id", all = TRUE)
df <- merge(df, public_metrics, by = "id", all = TRUE)
df <- merge(df, media_keys, by = "id", all = TRUE)
df <- merge(df, poll_ids, by = "id", all = TRUE)
df <- merge(df, geo, by = "id", all = TRUE)

write_rds(df, "data/all_data.Rds")