
# cargar  ----

## formato largo ----

censo_viv_comunas <- arrow::read_csv2_arrow("datos/censo_vivienda_comunas.csv")

censo_viv_comunas |> 
  filter(variable == "p05",
         nombre_comuna == "Timaukel")


## formato ancho ----
censo_viv <- arrow::read_parquet("datos/censo_vivienda.parquet")

censo_viv |> 
  filter(nombre_comuna == "Timaukel") |> 
  count(p05)
