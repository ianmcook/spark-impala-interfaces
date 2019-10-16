# Copyright 2019 Cloudera, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# This script requires that the Impala ODBC driver is installed on the host
# Modify .odbc.ini and .odbcinst.ini before running

if(!"implyr" %in% rownames(installed.packages())) {
  install.packages("implyr")
}
if(!"odbc" %in% rownames(installed.packages())) {
  install.packages("odbc")
}
if(!"shiny" %in% rownames(installed.packages())) {
  install.packages("shiny")
}
if(!"DT" %in% rownames(installed.packages())) {
  install.packages("DT")
}

library(implyr)
library(dplyr)
library(shiny)
library(DT)

impala <- src_impala(odbc::odbc(), dsn = "Impala DSN")

airlines <-
  tbl(impala, in_schema("default","airlines"))
flights <- 
  tbl(impala, in_schema("default","flights"))

summary_table <- airlines %>%
  left_join(flights, by = "carrier") %>%
  group_by(carrier, name) %>%
  summarise(
    num_flights = n(),
    num_destns = n_distinct(dest)
  ) %>%
  arrange(desc(num_flights)) %>%
  collect()

ui <- fluidPage(
  title = "Flights Drilldown",
  dataTableOutput("summary", height = 5),
  dataTableOutput("drilldown")
)

server <- function(input, output){
  output$summary <- renderDataTable(
    summary_table,
    selection = "single",
    options = list(pageLength = 4)
  )

  # subset the records to the row that was clicked
  drilldata <- reactive({
    shiny::validate(
      need(
        length(input$summary_rows_selected) > 0,
        "\nSelect a row to drill down"
      )
    )
    selected_carrier <- 
      summary_table[as.integer(input$summary_rows_selected), ]$carrier
    flights %>% 
      filter(carrier == !!selected_carrier) %>%
      #head(100) %>%
      collect()
  })

  # display the subsetted data
  output$drilldown <- renderDataTable(
    drilldata(),
    options = list(pageLength = 7)
  )
}

shinyApp(
  ui,
  server,
  options = list(
    port=as.numeric(Sys.getenv("CDSW_PUBLIC_PORT")),
    host=Sys.getenv("CDSW_IP_ADDRESS"),
    launch.browser="FALSE"
  )
)

dbDisconnect(impala)
