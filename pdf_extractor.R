# Read and process datasets in PDF files.

# VX0006370.pdf           (check)
# 29032737.pdf            (check)
# mx30004597.pdf          (check)
# Invoice# 0000420711.pdf (check)
# Invoice# 0000420712.pdf (check)
# 422678.pdf              (check)
# 0000427409.pdf          (check)
# F. 34015341.pdf         (check)

# Packages ----
library(tabulizer)
library(dplyr)
library(purrr)
library(readr)
fix_colnames <- function(clnms){
  make.names(clnms, unique = TRUE) %>%
    iconv(to = 'ASCII//TRANSLIT') %>%
    gsub("[^[:alnum:]]", "", .) %>%
    tolower}

# Part I: Extract data from PDFs ----
# VX0006370.pdf ----

out <- extract_tables("pdfs/VX0006370.pdf",
                      columns = list(c(85.9, 139.8, 170.7, 384, 423, 507, 581, 611)),
                      guess = FALSE,
                      output = "data.frame")  # Coords found with locate_areas("pdfs/VX0006370.pdf", pages = 1)

# All this chunk is no longer necessary since I've changed the way the data is read
# out <- extract_tables("pdfs/VX0006370.pdf", output = "data.frame")
# #map_chr(out, ~names(.)[[1]])
# out_df1 <- out[(map(out, names) %>% map_chr(1)) == "REFERENCIA"] %>%
#   map(mutate_all, funs(as.character)) %>%
#   bind_rows() %>%
#   as_tibble()  # %>% rename()  # only if necessary

out_df1 <- out %>%
  map(rename_all, funs(c("referencia", "product", "unids", "description",
                         "pais", "prec_unitario", "total", "var_to_rmv"))) %>%
  map(select, -8) %>%
  bind_rows %>%
  filter(grepl("\\d+\\.\\d{4}$", prec_unitario)) %>%
  as_tibble %>%
  mutate(moneda = "USD",
         filename = "VX0006370.pdf",
         pos_fact = row_number())

out_df1  # to-do: Clean some columns (remove chrs., e.g., "$")

# 29032737.pdf ----
# out <- extract_tables("pdfs/29032737.pdf",
#                       area = list(c(258, 20, 589, 758)),
#                       guess = FALSE,
#                       output = "data.frame")  # coordinates found with: locate_areas("pdfs/29032737.pdf")
#out <- extract_areas("pdfs/29032737.pdf", output = "data.frame")  # works, but requires selection (miniUI)
#out <- extract_text("pdfs/29032737.pdf")
# summary(out)
# out[[1]] %>% as_tibble()
# str(out)
out <- extract_tables("pdfs/29032737.pdf",
                      columns = list(c(50, 90, 145, 268, 386, 436, 467,
                                       498.3, 551, 579, 612, 660, 702.9, 758)),
                      guess = FALSE,
                      output = "data.frame")  # coordinates found with: locate_areas("pdfs/29032737.pdf")

# out_df2 <- out[(map(out, 1) %>% map(nchar) %>% map_dbl(mean, na.rm = TRUE)) < 5] %>%
#   map(mutate_all, funs(as.character)) %>%
#   bind_rows %>%
#   as_tibble %>%
#   filter(nchar(DESCRIPCION) > 10)
out_df2 <- out %>%
  map(rename_all, funs(c("sector", "product", "cod_barras", "descripcion", "pedimentos", "precio_sugerido",
                         "unid_x_caja", "unids", "clave_prod_serv", "clave_unidad", "unidad_medida",
                         "prec_unitario", "precio_umv", "total"))) %>%
  map(filter, sector == "0") %>%
  bind_rows %>%
  as_tibble %>%
  mutate(moneda = "USD",
         filename = "29032737.pdf",
         pos_fact = row_number())

out_df2  # To-do: colclass

# mx30004597.pdf ----
out <- extract_tables("pdfs/mx30004597.pdf",
                      columns = list(c(50, 90, 145, 268, 386, 436, 467,
                                       498.3, 551, 579, 612, 660, 702.9, 758)),
                      guess = FALSE,
                      output = "data.frame")  # coordinates: locate_areas("pdfs/mx30004597.pdf", pages = 1)

out_df3 <- out %>%
  map(rename_all, funs(c("sector", "product", "cod_barras", "descripcion", "pedimentos", "precio_sugerido",
                         "unid_x_caja", "unids", "clave_prod_serv", "clave_unidad", "unidad_medida",
                         "prec_unitario", "precio_umv", "total"))) %>%
  map(filter, sector == "0") %>%
  bind_rows %>%
  as_tibble %>%
  mutate(moneda = "USD",
         filename = "mx30004597.pdf",
         pos_fact = row_number())

out_df3  # To-do: colclass

# Invoice# 0000420711.pdf ----
out <- extract_tables("pdfs/Invoice# 0000420711.pdf",
                      columns = list(c(59.8, 121.5, 302, 355.5, 396.6, 449, 508, 570)),
                      guess = FALSE,
                      output = "data.frame")  # coordinates: locate_areas("pdfs/Invoice# 0000420711.pdf", pages = 1)

out_df4 <- out %>%
  map(rename_all, funs(c("line", "product", "description", "qty_ordered",
                         "unids", "qty_backord", "prec_unitario", "total"))) %>%
  map(filter, grepl("\\d+,\\d{2}", total)) %>%
  bind_rows %>%
  as_tibble %>%
  filter(line != "") %>%
  mutate(moneda = "USD",
         filename = "0000420711.pdf",
         pos_fact = row_number())

out_df4  # To-do: replace commas with a period. Colclass

# Invoice# 0000420712.pdf ----
out <- extract_tables("pdfs/Invoice# 0000420712.pdf",
                      columns = list(c(59.8, 121.5, 302, 355.5, 396.6, 449, 508, 570)),
                      guess = FALSE,
                      output = "data.frame")  # coordinates: locate_areas("pdfs/Invoice# 0000420711.pdf", pages = 1)

out_df5 <- out %>%
  map(rename_all, funs(c("line", "product", "description", "qty_ordered",
                         "unids", "qty_backord", "prec_unitario", "total"))) %>%
  map(filter, grepl("\\d+,\\d{2}", total)) %>%
  bind_rows %>%
  as_tibble %>%
  filter(line != "") %>%
  mutate(moneda = "USD",
         filename = "0000420712.pdf",
         pos_fact = row_number())

out_df5  # To-do: replace commas with a period. Colclass

# 422678.pdf SKINCEUTICALS ----
out <- extract_tables("pdfs/422678.pdf",
                      columns = list(c(67, 272.1, 336.3, 390.2, 442, 511.7, 571.3)),
                      guess = FALSE,
                      output = "data.frame")  # coordinates: locate_areas("pdfs/----421044.pdf", pages = 1)

out_df6 <- out %>%
  map(rename_all, funs(c("product", "description", "qty_ordered", "unids",
                         "Xqty_bkord-rtl_priceX", "prec_unitario", "total"))) %>%
  map(filter, grepl("\\d+,\\d{2}", total)) %>%
  bind_rows %>%
  as_tibble %>%
  filter(nchar(product) == 8 & grepl("^S", product)) %>%  # filter(product != "")
  mutate(moneda = "USD",
         filename = "422678.pdf",
         pos_fact = row_number())

out_df6  # To-do: replace commas with a period. Colclass

# 0000427409.pdf LOREAL PARIS----
out <- extract_tables("pdfs/0000427409.pdf",
                      columns = list(c(64.7, 245.5, 296, 353, 407, 470, 515.5, 569.6)),
                      guess = FALSE,
                      output = "data.frame")  # coordinates: locate_areas("pdfs/----421044.pdf", pages = 1)

out_df7 <- out %>%
  map(rename_all, funs(c("product", "description", "qty_ordered", "unids",
                         "reg_price", "per_unit_allow", "prec_unitario", "total"))) %>%
  map(filter, grepl("\\d+,\\d{2}", total),
              nchar(product) == 8,
              grepl("^K", product)) %>%
  bind_rows %>%
  as_tibble %>%
  mutate(moneda = "USD",
         filename = "0000427409.pdf",
         pos_fact = row_number())

out_df7  # To-do: replace commas with a period. Colclass

# F. 34015341.pdf BURGOS (needs rotation) ----
out <- extract_tables("pdfs/F. 34015341-rotated.pdf",
                      columns = list(c(80.7, 330.5, 400.5, 457, 508.6, 556, 592.3, 647.2, 697.4, 751, 816.2)),
                      guess = FALSE,
                      output = "data.frame")  # coordinates: locate_areas("pdfs/F. 34015341-rotated.pdf", pages = 1)

out_df8 <- out %>%
  map(rename_all, funs(c("product", "description", "custom_code", "vat", "origin", "alc_vol_tot",
                         "alc_degree", "wt_net_kg", "prec_unitario", "unids", "total"))) %>%
  map(filter, grepl("\\d+,\\d{2}", total),
      nchar(product) == 8,
      grepl("^E", product)) %>%
  bind_rows %>%
  as_tibble %>%
  mutate(moneda = "EUR",
         filename = "F. 3401541.pdf",
         pos_fact = row_number())

out_df8  # To-do: replace commas with a period. Colclass

# Part II: data cleaning ----
# 1) Choose cols
# 2) Clean cols
# 3) Coerce classes

# VX0006370.pdf VOGUE ----
out_df1 <- out_df1 %>%
  select(pos_fact, filename, product, unids, prec_unitario, total, moneda) %>%
  mutate(product = gsub("\\.", "", product),
         prec_unitario = readr::parse_number(prec_unitario),
         total = readr::parse_number(total),
         unids = as.integer(unids))

# 29032737.pdf MEX SLP ----
out_df2 <- out_df2 %>%
  select(pos_fact, filename, product, unids, prec_unitario, total, moneda) %>%
  mutate(prec_unitario = readr::parse_number(prec_unitario),
         total = readr::parse_number(total),
         unids = as.integer(unids))


# mx30004597.pdf MEX COSBEL ----
out_df3 <- out_df3 %>%
  select(pos_fact, filename, product, unids, prec_unitario, total, moneda) %>%
  mutate(prec_unitario = readr::parse_number(prec_unitario),
         total = readr::parse_number(total),
         unids = as.integer(unids))

# Invoice# 0000420711.pdf LUSA RDK ----
out_df4 <- out_df4 %>%
  select(pos_fact, filename, product, unids, prec_unitario, total, moneda) %>%
  mutate(prec_unitario = readr::parse_number(prec_unitario,
                                             locale = readr::locale(decimal_mark = ",", grouping_mark = ".")),
         total = readr::parse_number(total,
                                     locale = readr::locale(decimal_mark = ",", grouping_mark = ".")),
         unids = as.integer(unids))

# Invoice# 0000420712.pdf LUSA RDK ----
out_df5 <- out_df5 %>%
  select(pos_fact, filename, product, unids, prec_unitario, total, moneda) %>%
  mutate(prec_unitario = readr::parse_number(prec_unitario,
                                             locale = readr::locale(decimal_mark = ",", grouping_mark = ".")),
         total = readr::parse_number(total,
                                     locale = readr::locale(decimal_mark = ",", grouping_mark = ".")),
         unids = as.integer(unids))

# 422678.pdf LUSA SKINCEUTICALS ----
out_df6 <- out_df6 %>%
  select(pos_fact, filename, product, unids, prec_unitario, total, moneda) %>%
  mutate(prec_unitario = readr::parse_number(prec_unitario,
                                             locale = readr::locale(decimal_mark = ",", grouping_mark = ".")),
         total = readr::parse_number(total,
                                     locale = readr::locale(decimal_mark = ",", grouping_mark = ".")),
         unids = as.integer(unids))

# 0000427409.pdf LUSA LOREAL PARIS----
out_df7 <- out_df7 %>%
  select(pos_fact, filename, product, unids, prec_unitario, total, moneda) %>%
  mutate(prec_unitario = readr::parse_number(prec_unitario,
                                             locale = readr::locale(decimal_mark = ",", grouping_mark = ".")),
         total = readr::parse_number(total,
                                     locale = readr::locale(decimal_mark = ",", grouping_mark = ".")),
         unids = as.integer(unids))

# F. 34015341.pdf BURGOS (needs rotation) ----
out_df8 <- out_df8 %>%
  select(pos_fact, filename, product, unids, prec_unitario, total, moneda) %>%
  mutate(prec_unitario = readr::parse_number(prec_unitario,
                                             locale = readr::locale(decimal_mark = ",", grouping_mark = ".")),
         total = readr::parse_number(total,
                                     locale = readr::locale(decimal_mark = ",", grouping_mark = ".")),
         unids = as.integer(unids))

## En el caso de Burgos, para el TOTAL sólo considerar "TOTAL ALCOHOLIC PRODUCTS" + "TOTAL ADVERTISING PRODUCTS" + "TOTAL SALE PRODUCTS"

# Part III: Bind dfs by row ----
dfs_all <- bind_rows(out_df1, out_df2, out_df3, out_df4,
                     out_df5, out_df6, out_df7, out_df8)

# Part IV: Merge with ZVM03 ----
zvm03df <- read.csv("ZVM03-muestra_anita-noel.csv", colClasses = "character", stringsAsFactors = FALSE) %>%
  rename_all(fix_colnames) %>%
  select(matproveedor, ean13, textobrevematerial, descregistrosanitario, cpe, comentario, codregistrosanitario,
         feinicioregistro, fefinalregistro, signature, paisfabricante, paisdeorigen, formula, codfill,
         proveedor, cantidadentrega, preciototal) %>%
  mutate(cantidadentrega = parse_number(cantidadentrega) %>% as.integer()) %>%
  as_tibble()

cruce <- dfs_all %>% left_join(zvm03df, by = c("product" = "matproveedor", "unids" = "cantidadentrega")) %>%
  distinct(filename, pos_fact, .keep_all = TRUE)

###############################
# Relevant columns for final report:
# - Product code
# - Units (qty shipped)
# - Price (net_price or unit_price)
# - Total (USD)
# Conversation 20181010

### Code to get the class of every column in every dataset
map(out, ~map_chr(., class))

readr::write_csv(out_df_burgos, "burgos34016205.csv")