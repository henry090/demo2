
library(dplyr)
library(rvest)
library(stringr)
library(lubridate)
library(glue)
library(plyr)
library(doParallel)
library(data.table)

result11 <- data.frame(matrix(NA, nrow = 1, ncol = 1)) 

# get pages
colnames(result11) <- c("reference")

for (i in 1:nrow(result11)) {
  q <- enexpr(i)
  url <- glue('https://yeniemlak.az/elan/axtar?emlak=1&elan_nov=1&seher=0&metro=0&qiymet=&qiymet2=&mertebe=&mertebe2=&otaq=&otaq2=&sahe_m=&sahe_m2=&sahe_s=&sahe_s2=&page={enexpr(q)}')
  result11[i, 1] <- url
}

# get links per page
datalist11 = list()

for (i in 1:nrow(result11)) {
  page <- read_html(result11[i,])
  df <- html_attr(html_nodes(page, "a"), "href") %>% 
    grep("/elan/satilir",.,value = T) %>% 
    unique() %>% 
    as.data.frame() %>% 
    `colnames<-`(c('link'))
  datalist11[[i]] <- df
  print(i)
  Sys.sleep(10)
}

big_data13 = do.call(rbind, datalist11) %>% as.data.frame()


as.character(paste0('https://yeniemlak.az',as.character(big_data13$link))) -> big_data13$link

total = big_data13
total %>% distinct(link,.keep_all = T) -> total


bina_links = total$link

checking = paste("imgs/id_",gsub('https://yeniemlak.az/elan/','',bina_links),sep = '')

drs = list.dirs('imgs')

rmm = drs[drs %in% checking] %>% gsub('imgs/id_','',.) %>% paste("https://yeniemlak.az/elan/",.,sep = '')

bina_links = bina_links[!bina_links %in% rmm]

rm(big_data13,datalist11,df,i,page,q,result11,total,url)

for (i in 1:length(bina_links)) {
  baxis_tarix_elan = read_html(bina_links[i]) %>% 
    html_nodes("titem") %>%
    html_text()
  
  date=grep('Tarix',baxis_tarix_elan,value = T) %>% 
    str_extract(.,'(\\d+).(\\d+).(\\d+)') %>% 
    as.Date(format='%d.%m.%Y')
  
  txt=read_html("https://www.worldtimeserver.com/current_time_in_AZ.aspx?city=Baku") %>% 
    html_nodes('h4') %>% html_text() %>% 
    str_extract('([A-z]+) (\\d+), (\\d+)') %>% 
    gsub(',','',.) %>% 
    trimws()
  
  res=paste(match(str_extract(txt,'[A-z]+'), month.name),
            str_extract(txt,'(\\d+) (\\d+)')) %>% 
    as.Date(.,format='%m %d %Y')
  
  bina_links_ = ifelse(res==date, bina_links[i], NA)
  bina_links[i] = bina_links_
  print(paste(i,'out of',length(bina_links)))
  Sys.sleep(10)
}

bina_links = bina_links[!is.na(bina_links)]

length(bina_links)
rm(i,txt,bina_links_)

# Links
concat2 = as.data.frame(bina_links) 
colnames(concat2) = 'links'

list_to_gather = list()

for (i in 1:length(bina_links)) {
  html_page = read_html(bina_links[i])
  
  baxis_tarix_elan = html_page %>% 
    html_nodes("titem") %>%
    html_text()
  params=html_page %>% 
    html_nodes(".params") %>%
    html_text()
  
  res = grep('Elan',baxis_tarix_elan,value = T) %>% 
    str_extract(.,'\\d+') %>% as.numeric() %>% 
    #date
    append(., grep('Tarix',baxis_tarix_elan,value = T) %>% 
             str_extract(.,'(\\d+).(\\d+).(\\d+)') %>% 
             as.Date(format='%d.%m.%Y')) %>% 
    # yeni/kohne
    append(., html_page %>% 
             html_nodes(".box") %>%
             html_text() %>% .[1] %>% 
             str_extract('(Köhnə tikili)|(Yeni tikili)') ) %>% 
    # mertebe
    append(., grep('Mərtəbə',params,value = T) %>% 
             gsub('Mərtəbə','',.) %>% trimws()) %>% 
    # kvadrat
    append(., grep('(\\d+) m2',params,value = T) %>% 
             gsub(' m2','',.)) %>% 
    # otaq 
    append(., grep('(\\d+) otaq',params,value = T) %>% 
             gsub(' otaq','',.)) %>% 
    # kupca 
    append(., ifelse(is.na(grep('Kupça',params,value = T)),NA,
                     grep('Kupça',params,value = T)) ) %>% 
    # qiymet
    append(., html_page %>% 
             html_nodes("price") %>%
             html_text() %>% trimws()) %>% 
    # satici
    append(., html_page %>% 
             html_nodes(".elvrn") %>%
             html_text() %>% trimws()) %>% 
    # flat desc 
    append(., html_page %>% 
             html_nodes(".text") %>%
             html_text() %>% .[1]) %>% 
    # views
    append(., grep('Baxış sayı',baxis_tarix_elan,value = T) %>% 
             str_extract(.,'\\d+') %>% as.numeric()) %>% 
    # locations
    append(., params[!str_detect(params,'otaq|m2|Mərtəbə|Kupça')])
  
  list_to_gather[[i]] <- res
  print(paste('Collected',i,'out of',length(bina_links)))
  Sys.sleep(10)
}


new_df2=plyr::ldply(list_to_gather, rbind)

colnames(new_df2) <- c('id','date',paste('V',1:(ncol(new_df2)-2),sep = ''))

ress = list.files(pattern = 'houses',full.names = T)
ress2 = list.files(pattern = 'houses',full.names = F)

if(length(ress)>0) {
  ress = lapply(1:length(ress), function(x) file.info(ress[1])) %>% 
    do.call(rbind,.) %>% as.data.frame() %>% 
    mutate(smn = ress2) %>% 
    mutate(ctime=ymd_hms(ctime)) %>% filter(ctime==max(ctime)) %>% .[1,]
}

if(!file.exists('houses.csv')) {
  fwrite(new_df2,'houses.csv')
} else if (file.exists('houses.csv') & file.info(rownames(ress))$size/1e6 >= 60) {
  #dataset = fread('houses.csv')
  #total = rbind.fill(dataset,new_df2)
  nm = paste(round(runif(2),4), sep = '_',collapse = '_')
  fwrite(new_df2,paste('houses_',nm,'.csv',sep = ''))
} else if (file.exists('houses.csv') & file.info(rownames(ress))$size/1e6 < 60){
  dataset = fread(ress$smn)
  total = rbind.fill(dataset,new_df2)
  fwrite(total, file=ress$smn)
} else {
  stop('Something is wrong',call. = FALSE)
}

if(!dir.exists('imgs')) {
  dir.create('imgs')
}

setwd('imgs')
img_add_gather = list()

for (i in 1:length(bina_links)) {
  imgs = read_html(bina_links[i]) %>% 
    html_nodes('.img') %>% html_attr('src') %>% #paste('https://yeniemlak.az',.,sep = '') %>% 
    unique()
  idx = sample(1:length(imgs), ceiling(length(imgs)*0.8), replace=TRUE)
  dir_name = paste('id_',gsub('https://yeniemlak.az/elan/','',bina_links[i]),sep = '')
  dir.create(dir_name)
  imgs_=imgs
  imgs <- paste('https://yeniemlak.az',imgs,sep = '')
  for(j in idx) {
    download.file(imgs[j],destfile = file.path(
      dir_name,
      basename( paste('id_',gsub('/get-img/|.jpg','', imgs_[j]),
                      '_',
                      sample.int(1e6,1),sample.int(1e6,1),
                      '_img.jpg',sep='')
      )
    ))
  }
  print(paste('Done',i,'out of', length(bina_links)))
  Sys.sleep(10)
}




