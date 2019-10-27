library(shiny)
library(DT)
library(tabulizer)
library(dplyr)
library(purrr)
library(readr)

fix_colnames <- function(clnms){
  make.names(clnms, unique = TRUE) %>%
    iconv(to = 'ASCII//TRANSLIT') %>%
    gsub("[^[:alnum:]]", "", .) %>%
    tolower}

# Define UI for application that plots features of movies
ui <- fluidPage(
  
  # Application title
  titlePanel("Invoice Reader"),
  
  # Sidebar layout with a input and output definitions
  sidebarLayout(
    
    # Inputs(s)
    sidebarPanel(
      
      HTML("Ingresar los PDFs según la planta (proveedor):"),
      
      br(), br(),
      
      # Upload invoices for BURGOS - Productos Capilares Loreal
      fileInput(inputId = "burgos_pdfs", 
                label = "BURGOS - Prod. Capilares LOreal (requiere rotación)",
                multiple = TRUE,
                accept = "application/pdf"),
      
      # Upload invoices for VOGUE
      fileInput(inputId = "vogue_pdfs", 
                label = "VOGUE",
                multiple = TRUE,
                accept = "application/pdf"),
      
      # Upload invoices for L'Oreal Mexico SLP
      fileInput(inputId = "mexslp_pdfs", 
                label = "Mexico SLP",
                multiple = TRUE,
                accept = "application/pdf"),
      
      # Upload invoices for L'Oreal Mexico COSBEL
      fileInput(inputId = "mexcosbel_pdfs", 
                label = "Mexico COSBEL",
                multiple = TRUE,
                accept = "application/pdf"),
      
      # Upload invoices for LUSA RDK
      fileInput(inputId = "lusardk_pdfs", 
                label = "LUSA REDKEN",
                multiple = TRUE,
                accept = "application/pdf"),
      
      # Upload invoices for LUSA SKINCEUTICALS
      fileInput(inputId = "lusaskinceuticals_pdfs", 
                label = "LUSA SKINCEUTICALS",
                multiple = TRUE,
                accept = "application/pdf"),
      
      # Upload invoices for LUSA LOREAL PARIS
      fileInput(inputId = "lusalorealparis_pdfs", 
                label = "LUSA LOREAL PARIS",
                multiple = TRUE,
                accept = "application/pdf"),
      
      hr(),
      
      # Upload ZVM03
      fileInput(inputId = "zvm03_csv", 
                label = "ZVM03 from SAP",
                accept = c(
                  "text/csv",
                  "text/comma-separated-values,text/plain",
                  ".csv")),
      
      actionButton(inputId = "process_all",
                   label = "Procesar Todo")
      
    ),
    
    # Output(s)
    mainPanel(
      HTML("Una vez ingresados y procesados los PDFs, los datos se visualizarán abajo. Para descargar, presionar 'Descargar Datos'."),
      br(), br(),
      downloadButton(outputId = "download_data", label = "Descargar Datos"),
      br(), br(),
      DT::dataTableOutput(outputId = "invoices_table")
    )
  )
)

# Server
server <- function(input, output) {
  
  # BURGOS Reactive Expression
  burgos <- reactive({
    if(is.null(input$burgos_pdfs)){
      return(data.frame(pos_fact = integer(), filename = character(), product = character(),
                        unids = integer(), prec_unitario = double(), total = double(),
                        moneda = character()))
    } else {
      # Read data from pdfs
      #### PART I & PART II
      map2_dfr(input$burgos_pdfs$datapath, input$burgos_pdfs$name, function(pth, nm){
        extract_tables(pth,
                       columns = list(c(80.7, 330.5, 400.5, 457, 508.6, 556, 592.3, 647.2, 697.4, 751, 816.2)),
                       guess = FALSE,
                       output = "data.frame") %>%
          map(rename_all, funs(c("product", "description", "custom_code", "vat", "origin", "alc_vol_tot",
                                 "alc_degree", "wt_net_kg", "prec_unitario", "unids", "total"))) %>%
          map(filter, grepl("\\d+,\\d{2}", total),
              nchar(product) == 8,
              grepl("^E", product)) %>%
          bind_rows %>%
          as_tibble %>%
          mutate(moneda = "EUR",
                 filename = nm,
                 pos_fact = row_number()) %>%
          select(pos_fact, filename, product, unids, prec_unitario, total, moneda) %>%
          mutate(prec_unitario = readr::parse_number(prec_unitario,
                                                     locale = readr::locale(decimal_mark = ",", grouping_mark = ".")),
                 total = readr::parse_number(total,
                                             locale = readr::locale(decimal_mark = ",", grouping_mark = ".")),
                 unids = as.integer(unids))
      })
    }
  })
  
  # VOGUE Reactive Expression
  vogue <- reactive({
    if(is.null(input$vogue_pdfs)){
      return(data.frame(pos_fact = integer(), filename = character(), product = character(),
                        unids = integer(), prec_unitario = double(), total = double(),
                        moneda = character()))
    } else {
      # Read data from pdfs
      #### PART I & PART II
      map2_dfr(input$vogue_pdfs$datapath, input$vogue_pdfs$name, function(pth, nm){
        extract_tables(pth,
                       columns = list(c(85.9, 139.8, 170.7, 384, 423, 507, 581, 611)),
                       guess = FALSE,
                       output = "data.frame")%>%
          map(rename_all, funs(c("referencia", "product", "unids", "description",
                                 "pais", "prec_unitario", "total", "var_to_rmv"))) %>%
          map(select, -8) %>%
          bind_rows %>%
          filter(grepl("\\d+\\.\\d{4}$", prec_unitario)) %>%
          as_tibble %>%
          mutate(moneda = "USD",
                 filename = nm,
                 pos_fact = row_number()) %>%
          select(pos_fact, filename, product, unids, prec_unitario, total, moneda) %>%
          mutate(product = gsub("\\.", "", product),
                 prec_unitario = readr::parse_number(prec_unitario),
                 total = readr::parse_number(total),
                 unids = as.integer(unids))
      })
    }
  })
  
  # LOREAL MEXICO SLP Reactive Expression
  mexslp <- reactive({
    if(is.null(input$mexslp_pdfs)){
      return(data.frame(pos_fact = integer(), filename = character(), product = character(),
                        unids = integer(), prec_unitario = double(), total = double(),
                        moneda = character()))
    } else {
      # Read data from pdfs
      #### PART I & PART II
      map2_dfr(input$mexslp_pdfs$datapath, input$mexslp_pdfs$name, function(pth, nm){
        extract_tables(pth,
                       columns = list(c(50, 90, 145, 268, 386, 436, 467,
                                        498.3, 551, 579, 612, 660, 702.9, 758)),
                       guess = FALSE,
                       output = "data.frame") %>%
          map(rename_all, funs(c("sector", "product", "cod_barras", "descripcion", "pedimentos", "precio_sugerido",
                                 "unid_x_caja", "unids", "clave_prod_serv", "clave_unidad", "unidad_medida",
                                 "prec_unitario", "precio_umv", "total"))) %>%
          map(filter, sector == "0") %>%
          bind_rows %>%
          as_tibble %>%
          mutate(moneda = "USD",
                 filename = nm,
                 pos_fact = row_number()) %>%
          select(pos_fact, filename, product, unids, prec_unitario, total, moneda) %>%
          mutate(prec_unitario = readr::parse_number(prec_unitario),
                 total = readr::parse_number(total),
                 unids = as.integer(unids))
      })
    }
  })
  
  # LOREAL MEXICO COSBEL Reactive Expression
  mexcosbel <- reactive({
    if(is.null(input$mexcosbel_pdfs)){
      return(data.frame(pos_fact = integer(), filename = character(), product = character(),
                        unids = integer(), prec_unitario = double(), total = double(),
                        moneda = character()))
    } else {
      # Read data from pdfs
      #### PART I & PART II
      map2_dfr(input$mexcosbel_pdfs$datapath, input$mexcosbel_pdfs$name, function(pth, nm){
        extract_tables(pth,
                       columns = list(c(50, 90, 145, 268, 386, 436, 467,
                                        498.3, 551, 579, 612, 660, 702.9, 758)),
                       guess = FALSE,
                       output = "data.frame") %>%
          map(rename_all, funs(c("sector", "product", "cod_barras", "descripcion", "pedimentos", "precio_sugerido",
                                 "unid_x_caja", "unids", "clave_prod_serv", "clave_unidad", "unidad_medida",
                                 "prec_unitario", "precio_umv", "total"))) %>%
          map(filter, sector == "0") %>%
          bind_rows %>%
          as_tibble %>%
          mutate(moneda = "USD",
                 filename = nm,
                 pos_fact = row_number()) %>%
          select(pos_fact, filename, product, unids, prec_unitario, total, moneda) %>%
          mutate(prec_unitario = readr::parse_number(prec_unitario),
                 total = readr::parse_number(total),
                 unids = as.integer(unids))
      })
    }
  })
  
  # LUSA RDK Reactive Expression
  lusardk <- reactive({
    if(is.null(input$lusardk_pdfs)){
      return(data.frame(pos_fact = integer(), filename = character(), product = character(),
                        unids = integer(), prec_unitario = double(), total = double(),
                        moneda = character()))
    } else {
      # Read data from pdfs
      #### PART I & PART II
      map2_dfr(input$lusardk_pdfs$datapath, input$lusardk_pdfs$name, function(pth, nm){
        extract_tables(pth,
                       columns = list(c(59.8, 121.5, 302, 355.5, 396.6, 449, 508, 570)),
                       guess = FALSE,
                       output = "data.frame") %>%
          map(rename_all, funs(c("line", "product", "description", "qty_ordered",
                                 "unids", "qty_backord", "prec_unitario", "total"))) %>%
          map(filter, grepl("\\d+,\\d{2}", total)) %>%
          bind_rows %>%
          as_tibble %>%
          filter(line != "") %>%
          mutate(moneda = "USD",
                 filename = nm,
                 pos_fact = row_number()) %>%
          select(pos_fact, filename, product, unids, prec_unitario, total, moneda) %>%
          mutate(prec_unitario = readr::parse_number(prec_unitario,
                                                     locale = readr::locale(decimal_mark = ",", grouping_mark = ".")),
                 total = readr::parse_number(total,
                                             locale = readr::locale(decimal_mark = ",", grouping_mark = ".")),
                 unids = as.integer(unids))
      })
    }
  })
  
  # LUSA SKINCEUTICALS Reactive Expression
  lusaskinceuticals <- reactive({
    if(is.null(input$lusaskinceuticals_pdfs)){
      return(data.frame(pos_fact = integer(), filename = character(), product = character(),
                        unids = integer(), prec_unitario = double(), total = double(),
                        moneda = character()))
    } else {
      # Read data from pdfs
      #### PART I & PART II
      map2_dfr(input$lusaskinceuticals_pdfs$datapath, input$lusaskinceuticals_pdfs$name, function(pth, nm){
        extract_tables(pth,
                       columns = list(c(67, 272.1, 336.3, 390.2, 442, 511.7, 571.3)),
                       guess = FALSE,
                       output = "data.frame") %>%
          map(rename_all, funs(c("product", "description", "qty_ordered", "unids",
                                 "Xqty_bkord-rtl_priceX", "prec_unitario", "total"))) %>%
          map(filter, grepl("\\d+,\\d{2}", total)) %>%
          bind_rows %>%
          as_tibble %>%
          filter(nchar(product) == 8 & grepl("^S", product)) %>%  # filter(product != "")
          mutate(moneda = "USD",
                 filename = nm,
                 pos_fact = row_number()) %>%
          select(pos_fact, filename, product, unids, prec_unitario, total, moneda) %>%
          mutate(prec_unitario = readr::parse_number(prec_unitario,
                                                     locale = readr::locale(decimal_mark = ",", grouping_mark = ".")),
                 total = readr::parse_number(total,
                                             locale = readr::locale(decimal_mark = ",", grouping_mark = ".")),
                 unids = as.integer(unids))
      })
    }
  })
  
  # LUSA LOREAL PARIS Reactive Expression
  lusalorealparis <- reactive({
    if(is.null(input$lusalorealparis_pdfs)){
      return(data.frame(pos_fact = integer(), filename = character(), product = character(),
                        unids = integer(), prec_unitario = double(), total = double(),
                        moneda = character()))
    } else {
      # Read data from pdfs
      #### PART I & PART II
      map2_dfr(input$lusalorealparis_pdfs$datapath, input$lusalorealparis_pdfs$name, function(pth, nm){
        extract_tables(pth,
                       columns = list(c(64.7, 245.5, 296, 353, 407, 470, 515.5, 569.6)),
                       guess = FALSE,
                       output = "data.frame") %>%
          map(rename_all, funs(c("product", "description", "qty_ordered", "unids",
                                 "reg_price", "per_unit_allow", "prec_unitario", "total"))) %>%
          map(filter, grepl("\\d+,\\d{2}", total),
              nchar(product) == 8,
              grepl("^K", product)) %>%
          bind_rows %>%
          as_tibble %>%
          mutate(moneda = "USD",
                 filename = nm,
                 pos_fact = row_number()) %>%
          select(pos_fact, filename, product, unids, prec_unitario, total, moneda) %>%
          mutate(prec_unitario = readr::parse_number(prec_unitario,
                                                     locale = readr::locale(decimal_mark = ",", grouping_mark = ".")),
                 total = readr::parse_number(total,
                                             locale = readr::locale(decimal_mark = ",", grouping_mark = ".")),
                 unids = as.integer(unids))
      })
    }
  })
  
  # ZVM03
  zvm03 <- reactive({
    read.csv(input$zvm03_csv$datapath, colClasses = "character", stringsAsFactors = FALSE) %>%
      rename_all(fix_colnames) %>%
      select(matproveedor, ean13, textobrevematerial, descregistrosanitario, cpe, comentario, codregistrosanitario,
             feinicioregistro, fefinalregistro, signature, paisfabricante, paisdeorigen, formula, codfill,
             proveedor, cantidadentrega, preciototal) %>%
      mutate(cantidadentrega = parse_number(cantidadentrega) %>% as.integer()) %>%
      as_tibble()
  })
  
  # Reactive Expression after pressing button
  consolidado <- eventReactive(input$process_all, {
    
    progress <- shiny::Progress$new()
    
    on.exit(progress$close())
    
    progress$set(message = "Procesando", value = 1/2)
    
    if(is.null(input$zvm03_csv)){
      #### PART III
      bind_rows(burgos(),
                vogue(),
                mexslp(),
                mexcosbel(),
                lusardk(),
                lusaskinceuticals(),
                lusalorealparis())
    } else {
      #### Add part IV - cruce
      bind_rows(burgos(),
                vogue(),
                mexslp(),
                mexcosbel(),
                lusardk(),
                lusaskinceuticals(),
                lusalorealparis()) %>%
        left_join(zvm03(), by = c("product" = "matproveedor", "unids" = "cantidadentrega")) %>%
        distinct(filename, pos_fact, .keep_all = TRUE)
    }
  })
  
  output$invoices_table <- DT::renderDataTable({
    DT::datatable(data = consolidado(), 
                  options = list(pageLength = 10), 
                  rownames = FALSE)
  })
  
  # Download file
  output$download_data <- downloadHandler(
    filename = "consolidado.csv",
    content = function(file) { 
      write_csv(consolidado(), file) 
    }
  )
  
}

# Create the Shiny app object
shinyApp(ui = ui, server = server)