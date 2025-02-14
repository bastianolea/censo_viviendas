library(arrow)
library(dplyr)
library(tidyr)
library(janitor)
library(readr)

# cargar ----
datos <- read_csv2_arrow("datos/datos_originales/csv-viviendas-censo-2017/Microdato_Censo2017-Viviendas.csv") |> 
  clean_names()

datos

# cargar códigos únicos territoriales
cut_comunas <- read_delim_arrow("datos/datos_externos/cut_comuna.csv", delim = ";") |> 
  mutate(codigo_comuna = as.numeric(codigo_comuna)) |> 
  select(codigo_region, nombre_region, codigo_comuna, nombre_comuna)

# cargar clasificación de comunas pndr
clasificacion <- read_delim_arrow("datos/datos_externos/clasificacion_pndr.csv", delim = ";") |> 
  select(codigo_comuna, clasificacion)


# comunas ----
datos2 <- datos |> 
  rename(codigo_comuna = comuna) |> 
  select(-region, -provincia, -ends_with("15r")) |> 
  left_join(cut_comunas, by = "codigo_comuna") |> 
  left_join(clasificacion, by = "codigo_comuna") |> 
  relocate(codigo_region, nombre_region, codigo_comuna, nombre_comuna, clasificacion, 
           .before = 1)


# recodificar ----
datos3 <- datos2 |> 
  mutate(area = case_match(area,
                           1 ~ "Urbana",
                           2 ~ "Rural")) |> 
  mutate(p01 = case_match(p01,
                          1 ~ "Casa",
                          2 ~ "Departamento en edificio",
                          3 ~ "Vivienda tradicional indígena (ruka, pae pae u otras)",
                          4 ~ "Pieza en casa antigua o en conventillo",
                          5 ~ "Mediagua, mejora, rancho o choza",
                          6 ~ "Móvil (carpa, casa rodante o similar)",
                          7 ~ "Otro tipo de vivienda particular",
                          8 ~ "Vivienda colectiva",
                          9 ~ "Operativo personas en tránsito (no es vivienda)",
                          10 ~ "Operativo calle (no es vivienda)")) |> 
  
  mutate(p02 = case_match(p02,
                          1 ~ "Con moradores presentes",
                          2 ~ "Con moradores ausentes",
                          3 ~ "En venta, para arriendo, abandonada u otro",
                          4 ~ "De temporada (vacacional u otro)")) |> 
  
  mutate(p03a = case_match(p03a,
                           1 ~ "Hormigón armado",
                           2 ~ "Albañilería: bloque de cemento, piedra o ladrillo",
                           3 ~ "Tabique forrado por ambas caras (madera o acero) (1-6)",
                           4 ~ "Tabique sin forro interior (madera u otro)",
                           5 ~ "Adobe, barro, quincha, pirca u otro artesanal tradicional",
                           6 ~ "Materiales precarios (lata, cartón, plástico, etc.)",
                           98 ~ "No aplica",
                           99 ~ NA)) |> 
  
  mutate(p03b = case_match(p03b,
                           1 ~ "Tejas o tejuelas de arcilla, metálicas, de cemento, de madera, asfálticas o plásticas",
                           2 ~ "Losa hormigón",
                           3 ~ "Planchas metálicas de zinc, cobre, etc. o fibrocemento (tipo pizarreño)",
                           4 ~ "Fonolita o plancha de fieltro embreado",
                           5 ~ "Paja, coirón, totora o caña",
                           6 ~ "Materiales precarios (lata, cartón, plásticos, etc.)",
                           7 ~ "Sin cubierta sólida de techo",
                           98 ~ "No aplica",
                           99 ~ NA)) |> 
  mutate(p03c = case_match(p03c,
                           1 ~ "Parquet, piso flotante, cerámico, madera, alfombra, flexit, cubrepiso u otro similar, sobre radier o vigas de madera (1-5)",
                           2 ~ "Radier sin revestimiento",
                           3 ~ "Baldosa de cemento",
                           4 ~ "Capa de cemento sobre tierra",
                           5 ~ "Tierra",
                           98 ~ "No aplica",
                           99 ~ NA)) |> 
  mutate(p04 = case_match(p04,
                          0 ~ "0 piezas",
                          1 ~ "1 pieza",
                          2 ~ "2 piezas",
                          3 ~ "3 piezas",
                          4 ~ "4 piezas",
                          5 ~ "5 piezas",
                          6 ~ "6 o más piezas",
                          98 ~ "No aplica",
                          99 ~ NA)) |> 
  mutate(p05 = case_match(p05,
                          1 ~ "Red pública",
                          2 ~ "Pozo o noria",
                          3 ~ "Camión aljibe",
                          4 ~ "Río, vertiente, estero, canal, lago, etc.",
                          98 ~ "No aplica",
                          99 ~ NA))

# pivotar ----
datos4 <- datos3 |> 
  select(-dc, -zc_loc, -id_zona_loc, -nviv) |> 
  pivot_longer(cols = area:p05,
               names_to = "variable", values_to = "valor")

# conteo ----
datos5 <- datos4 |> 
  group_by(codigo_region, nombre_region, codigo_comuna, nombre_comuna, clasificacion,
           variable) |> 
  count(valor)


# guardar ----

# formato largo de conteo
write_csv2(datos5, "datos/censo_vivienda_comunas.csv")

# formato ancho
write_parquet(datos3, "datos/censo_vivienda.parquet")
