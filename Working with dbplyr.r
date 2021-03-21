if(!require(pacman)){ install.packages("pacman")}
pacman::p_load(DBI, tidyverse, keyring)

class(RPostgres::Postgres())

key_list()

con <- dbConnect(drv = RPostgres::Postgres(), user=key_get("pg_user"), password=key_get("pg_pwd"), dbname="chinook")

class(con)

#con <- dbConnect(drv = RPostgres::Postgres(), 
#          user=rstudioapi::askForPassword("Database user"), 
#          password=rstudioapi::askForPassword("Database password"), 
#          dbname=rstudioapi::askForPassword("Database name"))

dbListTables(conn = con)

dbListTables(con)

tbl(src = con, from = "Invoice")

tbl(src = con, from = "Invoice") %>% collect() %>% head()

tbl(src = con, from = "Invoice") %>% collect() %>% dim()

dbListTables(con) %>%
map(`.f` = ~tbl(src = con, from = .) %>% collect() %>% dim()) %>% 
tibble() %>% 
mutate(dbListTables(con)) %>% 
setNames(c("dims","tbls")) %>%
relocate(tbls, `.before` = everything())

customers <- tbl(src = con, from = "Customer") %>% collect()
dim(customers)

invoice_lines <- tbl(src = con, from = "InvoiceLine") %>% collect()

dim(invoice_lines)

glimpse(invoice_lines)

# Helper function
colct <- function(tb){
    tbl(src = con, from = tb) %>% collect()
}

colct("Track") %>% head()

purchase_info <- invoice_lines %>% 
# Track info
left_join(colct("Track"), by = "TrackId") %>% 
# Genre info
left_join(colct("Genre"), by = "GenreId") %>% 
# Album info
left_join(colct("Album"), by = "AlbumId") %>% 
# Artist info
left_join(colct("Artist"), by = "ArtistId") %>% 
# Invoice info
left_join(colct("Invoice"), by = "InvoiceId")

dim(purchase_info)

glimpse(purchase_info)

purchase_info <- purchase_info %>% 
mutate(InvoiceDate = as.Date(InvoiceDate))

skimr::skim(purchase_info)

purchase_info %>% 
summarise_all(`.funs` = list(complete = ~mean(!is.na(.)),
                             distinct = ~n_distinct(.),
                             class = ~class(.))) %>% 
t() %>% 
data.frame() %>% 
rownames_to_column() %>% 
setNames(c("rowname","value")) %>% 
extract(col = rowname, into = c("var","metric"), regex = "(^.*)_(.*)") %>% 
pivot_wider(names_from = metric, values_from = value) %>% 
mutate(obs = nrow(purchase_info)) %>% 
relocate(class, `.after` = var) %>% 
filter(!str_detect(string = var, pattern = ".y$"))

colnames(purchase_info)

purchase_info %>% 
select(CustomerId, Name) %>% 
count(CustomerId,Name, name = "times_bought") %>% 
group_by(CustomerId) %>% 
mutate(artists = n_distinct(Name)) %>% 
sample_n(1) %>% 
head()

purchase_info <- purchase_info %>% 
mutate_if(`.predicate` = is.character, `.funs` = str_trim)

artist_dum <- purchase_info %>% 
select(CustomerId,Name) %>% 
count(CustomerId,Name) %>% 
pivot_wider(names_from = Name, values_from = n, values_fill = 0, names_prefix = "artist_")

dim(artist_dum)

n_distinct(purchase_info$CustomerId)
dim(customers)

copy_to(dest = con, df = artist_dum, name = "customer_artists", temporary = FALSE, overwrite = TRUE)

dbListTables(conn = con)

dbListTables(conn = con)

db_tables_list <- c("InvoiceLine","Track","Genre","Album","Artist","Invoice","Customer")

db_tables <- db_tables_list %>% map(`.f` = ~tbl(src = con, from = .))

class(db_tables)

# Convert list items to objects
for(t in seq_along(db_tables)){
    assign(x = db_tables_list[t], value = db_tables[[t]])
}

InvoiceLine

Customer %>% 
show_query()

InvoiceLine %>% 
left_join(Track, by = "TrackId") %>% 
left_join(Album, by = "AlbumId") %>% 
left_join(Artist, by = "ArtistId") %>% 
left_join(Invoice, by = "InvoiceId") %>% 
left_join(Customer, by = "CustomerId") %>% 
collect() %>% 
dim()

InvoiceLine %>% 
left_join(Track, by = "TrackId") %>% 
left_join(Album, by = "AlbumId") %>% 
left_join(Artist, by = "ArtistId") %>% 
left_join(Invoice, by = "InvoiceId") %>% 
left_join(Customer, by = "CustomerId") %>% 
select(CustomerId,Name.y) %>% 
count(CustomerId,Name.y) %>% 
group_by(CustomerId) %>% 
mutate(artists_count = n()) %>% 
filter(CustomerId=="1") %>% 
summarise(artists_count = round(mean(artists_count)),
          purchase_count = sum(n)) %>%
show_query()

object.size(InvoiceLine)

object.size(InvoiceLine %>% collect())

dbDisconnect(con)

purchase_info %>% 
select_if(`.predicate` = is.numeric) %>% 
select(-contains("Invoice")) %>% 
pivot_longer(cols = everything(), names_to = "key", values_to = "value") %>% 
ggplot(aes(value))+
geom_histogram(bins = 10)+
facet_wrap(~key, scales = "free")

dbListTables(con)

tbl(src = con, from = "InvoiceLine") %>% 
mutate(const_int = 1,
       const_str = "abc",
       concat_str = paste0(const_int,"_",const_str),
       amount = UnitPrice * Quantity) %>% 
show_query()

tbl(src = con, from = "Invoice") %>% 
mutate(InvoiceDate = as.Date(InvoiceDate)) %>%
mutate(new_num = 04) %>% 
mutate(new_num = as.character(new_num)) %>% 
show_query()

tbl(src = con, from = "Customer") %>% 
collect() %>% 
summarise(across(`.cols` = where(is.character), `.fns` = ~sum(is.na(.))))

employee <- read_csv(file = "./data-1614526370848.csv") %>% 
mutate(manager_id = as.numeric(manager_id)) %>% 
mutate(both_names = paste0(first_name," ",last_name)) %>% 
select(-first_name, -last_name) %>% 
relocate(both_names, `.after` = employee_id) 

employee %>% summarise(across(`.cols` = everything(), `.fns` = class))

employee %>% 
left_join(employee, by = c("manager_id"="employee_id")) %>% 
select(-contains("id"))

tbl(src = con, from = "Customer") %>% 
filter(str_detect(string = FirstName, pattern = "Fran|Leon"))


