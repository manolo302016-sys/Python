## Segundo visualizador 2: gestion_saludmental (Dashboard Frontend Next.js)##

### (Crear otro fichero para identificar los scripts de este visualizador. Se crea una base de datos con los trabajadores evaluados asignando los resultados de estos pasos de calificacion y analisis) ##
## Paso 1. Estandarizar las escalas de todos los instrumentos en un rango 0 - 1. Los codigos asignados a las respuestas en el paso 1 y 2 del visualizador 1, se estandarizan en una escala 0 a 1.Tanto los items que se invirtieron como los que quedaron en su sentido original, se estandarizan en una escala 0 a 1. A cada trabajador se asigna su escala estandarizada en cada pregunta ##
## |forma_intra|id_pregunta|id_respuesta|Estandarizacion 0 a 1|##
## |intralaboral_A|1 a 105|4|1|##
## |intralaboral_A|1 a 105|3|0.75|##
## |intralaboral_A|1 a 105|2|0.5|##
## |intralaboral_A|1 a 105|1|0.25|##
## |intralaboral_A|1 a 105|0|0|##
## |intralaboral_A|106|1|1|##
## |intralaboral_A|106|0|0|##
## |intralaboral_A|107 a 115|4|1|##
## |intralaboral_A|107 a 115|3|0.75|##
## |intralaboral_A|107 a 115|2|0.5|##
## |intralaboral_A|107 a 115|1|0.25|##
## |intralaboral_A|107 a 115|0|0|##
## |intralaboral_A|116|1|1|##
## |intralaboral_A|116|0|0|##
## |intralaboral_A|117 a 125|4|1|##
## |intralaboral_A|117 a 125|3|0.75|##
## |intralaboral_A|117 a 125|2|0.5|##
## |intralaboral_A|117 a 125|1|0.25|##
## |intralaboral_A|117 a 125|0|0|##
## |intralaboral_B|1 a 88|4|1|##
## |intralaboral_B|1 a 88|3|0.75|##
## |intralaboral_B|1 a 88|2|0.5|##
## |intralaboral_B|1 a 88|1|0.25|##
## |intralaboral_B|1 a 88|0|0|##
## |intralaboral_B|89|1|1|##
## |intralaboral_B|89|0|0|##
## |intralaboral_B|90 a 98|4|1|##
## |intralaboral_B|90 a 98|3|0.75|##
## |intralaboral_B|90 a 98|2|0.5|##
## |intralaboral_B|90 a 98|1|0.25|##
## |intralaboral_B|90 a 98|0|0|##
## |extralaboral|1 a 31|4|1|##
## |extralaboral|1 a 31|3|0.75|##
## |extralaboral|1 a 31|2|0.5|##
## |extralaboral|1 a 31|1|0.25|##
## |extralaboral|1 a 31|0|0|##
## |estres|1,2,3,9,13,14,15,23,24|9|1|##
## |estres|1,2,3,9,13,14,15,23,24|6|0.66|##
## |estres|1,2,3,9,13,14,15,23,24|3|0.33|##
## |estres|1,2,3,9,13,14,15,23,24|0|0|##
## |estres|4,5,6,10,11,16,17,18,19,25,26,27,28|6|1|##
## |estres|4,5,6,10,11,16,17,18,19,25,26,27,28|4|0.66|##
## |estres|4,5,6,10,11,16,17,18,19,25,26,27,28|2|0.33|##
## |estres|4,5,6,10,11,16,17,18,19,25,26,27,28|0|0|##
## |estres|7,8,12,20,21,22,29,30,31|3|1|##
## |estres|7,8,12,20,21,22,29,30,31|2|0.66|##
## |estres|7,8,12,20,21,22,29,30,31|1|0.33|##
## |estres|7,8,12,20,21,22,29,30,31|0|0|##
## |afrontamiento|1 a 12|0|0|##
## |afrontamiento|1 a 12|0.5|0.33|##
## |afrontamiento|1 a 12|0.7|0.66|##
## |afrontamiento|1 a 12|1|1|## 
## |capital_psicologico|1 a 12|0|0|##
## |capital_psicologico|1 a 12|0.5|0.33|##
## |capital_psicologico|1 a 12|0.7|0.66|##
## |capital_psicologico|1 a 12|1|1|##

## Paso 2. Agrupar preguntas en ejes, lineas de gestión e indicadores del modelo de gestión##
## las preguntas se agrupan en indicadores, los indicadores se agrupan en líneas de gestión y las líneas de gestión se agrupan en ejes de gestión. A su vez, cada línea de gestión tiene asociado los protocolos de gestión, los cuales serán consumidos por el sistema RAG en fase posterior. La tabla corresponde a la tabla dimensional categorias_analisis, detalla la agrupación que debe aplicarse para el visualizador 2 del modelo conceptual de base, se detalla la pregunta, la forma a la que corresponde, el factor, dimension y pregunta que vienen de la agrupación original del modelo aplicado en el visualizador 1. Lo anterior agrupado en indicadores, líneas de gestión, protocolos y ejes de gestion. A cada trabajador se asigna las variables del modelo de gestion, para que en pasos posteriores,se asigne el resultado de cada componente del modelo ## 
## |id_pregunta | forma_intra| factor| dimension| pregunta_texto| indicador| protocolo_id| protocolo_nombre| linea_gestion| eje_gestion##

## |12_afrontamiento|A_y_B|Estrés - individual|Afrontamiento activo_busquedasoporte|Le pregunto a alguien con mayor experiencia sobre el tema|Activa red de apoyo|PROT-01|Afrontamiento del estrés y recursos psicológicos|Afrontamiento del estrés y recursos psicológicos|Bienestar biopsicosocial
## |5_afrontamiento|A_y_B|Estrés - individual|Afrontamiento evitativo_negación|Me digo a mí mismo: "esto no es real"|Autonegación|PROT-01|Afrontamiento del estrés y recursos psicológicos|Afrontamiento del estrés y recursos psicológicos|Bienestar biopsicosocial
## |9_afrontamiento|A_y_B|Estrés - individual|Afrontamiento activo_busquedasoporte|Busco alguna ayuda profesional|Busqueda de Apoyo profesional|PROT-01|Afrontamiento del estrés y recursos psicológicos|Afrontamiento del estrés y recursos psicológicos|Bienestar biopsicosocial
## |10_afrontamiento|A_y_B|Estrés - individual|Afrontamiento activo_busquedasoporte|Consigo que otras personas me ayuden o aconsejen|Busqueda de consejo y orientación|PROT-01|Afrontamiento del estrés y recursos psicológicos|Afrontamiento del estrés y recursos psicológicos|Bienestar biopsicosocial
## |11_afrontamiento|A_y_B|Estrés - individual|Afrontamiento activo_busquedasoporte|Consigo el consuelo o la comprensión de alguien|Busqueda de soporte emocional|PROT-01|Afrontamiento del estrés y recursos psicológicos|Afrontamiento del estrés y recursos psicológicos|Bienestar biopsicosocial
## |1_afrontamiento|A_y_B|Estrés - individual|Afrontamiento activo_planificación|Concentro mis esfuerzos en hacer algo sobre la situación en la que estoy|Dedicación|PROT-01|Afrontamiento del estrés y recursos psicológicos|Afrontamiento del estrés y recursos psicológicos|Bienestar biopsicosocial##
## |7_afrontamiento|A_y_B|Estrés - individual|Afrontamiento evitativo_negación|Me niego a creer que haya sucedido|Evitación cognitiva|PROT-01|Afrontamiento del estrés y recursos psicológicos|Afrontamiento del estrés y recursos psicológicos|Bienestar biopsicosocial
## |8_afrontamiento|A_y_B|Estrés - individual|Afrontamiento evitativo_negación|Intento distraerme o pensar en otra cosa|Evitación conductual|PROT-01|Afrontamiento del estrés y recursos psicológicos|Afrontamiento del estrés y recursos psicológicos|Bienestar biopsicosocial
## |2_afrontamiento|A_y_B|Estrés - individual|Afrontamiento activo_planificación|Tomo medidas para intentar que la situación mejore|Orientación a la acción|PROT-01|Afrontamiento del estrés y recursos psicológicos|Afrontamiento del estrés y recursos psicológicos|Bienestar biopsicosocial
## |4_afrontamiento|A_y_B|Estrés - individual|Afrontamiento activo_planificación|Pienso detenidamente sobre los pasos a seguir|Planificación|PROT-01|Afrontamiento del estrés y recursos psicológicos|Afrontamiento del estrés y recursos psicológicos|Bienestar biopsicosocial
## |3_afrontamiento|A_y_B|Estrés - individual|Afrontamiento activo_planificación|Intento proponer una estrategia sobre qué hacer|Propositividad|PROT-01|Afrontamiento del estrés y recursos psicológicos|Afrontamiento del estrés y recursos psicológicos|Bienestar biopsicosocial##
## |6_afrontamiento|A_y_B|Estrés - individual|Afrontamiento evitativo_negación|Renuncio a intentar ocuparme de ello|Renuncia|PROT-01|Afrontamiento del estrés y recursos psicológicos|Afrontamiento del estrés y recursos psicológicos|Bienestar biopsicosocial##
## |10_estres|A_y_B|Estrés - individual|Respuestas cognitivas de estrés|Dificultad para permanecer quieto o dificultad para iniciar actividades|Alteraciones cognitivas|PROT-02|Bienestar cognitivo|Bienestar cognitivo|Bienestar biopsicosocial##
## |14_estres|A_y_B|Estrés - individual|Respuestas cognitivas de estrés|Dificultad para concentrarse, olvidos frecuentes|Alteraciones cognitivas|PROT-02|Bienestar cognitivo|Bienestar cognitivo|Bienestar biopsicosocial##
## |21_estres|A_y_B|Estrés - individual|Respuestas cognitivas de estrés|Dificultad para tomar decisiones|Alteraciones cognitivas|PROT-02|Bienestar cognitivo|Bienestar cognitivo|Bienestar biopsicosocial##
## |23_estres|A_y_B|Estrés - individual|Respuestas emocionales de estrés|Sentimiento de soledad y miedo|Desgaste emocional|PROT-04|Bienestar emocional y trascendente — Intervención en trastornos|Bienestar emocional y trascendente|Bienestar biopsicosocial##
## |24_estres|A_y_B|Estrés - individual|Respuestas emocionales de estrés|Sentimiento de irritabilidad, actitudes y pensamientos negativos|Desgaste emocional|PROT-04|Bienestar emocional y trascendente — Intervención en trastornos|Bienestar emocional y trascendente|Bienestar biopsicosocial##
## |25_estres|A_y_B|Estrés - individual|Respuestas emocionales de estrés|Sentimiento de angustia, preocupación o tristeza|Desgaste emocional|PROT-04|Bienestar emocional y trascendente — Intervención en trastornos|Bienestar emocional y trascendente|Bienestar biopsicosocial##
## |29_estres|A_y_B|Estrés - individual|Respuestas emocionales de estrés|Sentimiento de que está perdiendo la razón|Desgaste emocional|PROT-04|Bienestar emocional y trascendente — Intervención en trastornos|Bienestar emocional y trascendente|Bienestar biopsicosocial##
## |31_estres|A_y_B|Estrés - individual|Respuestas emocionales de estrés|Sensación de no poder manejar los problemas de la vida|Desgaste emocional|PROT-04|Bienestar emocional y trascendente — Intervención en trastornos|Bienestar emocional y trascendente|Bienestar biopsicosocial##
## |29_extra|A_y_B|Extralaboral|Situación económica del grupo familiar|El dinero que ganamos en el hogar alcanza para cubrir los gastos básicos|Bienestar financiero|PROT-07|Bienestar financiero|Bienestar financiero|Bienestar biopsicosocial##
## |30_extra|A_y_B|Extralaboral|Situación económica del grupo familiar|Tengo otros compromisos económicos que afectan mucho el presupuesto familiar|Bienestar financiero|PROT-07|Bienestar financiero|Bienestar financiero|Bienestar biopsicosocial##
## |31_extra|A_y_B|Extralaboral|Situación económica del grupo familiar|En mi hogar tenemos deudas difíciles de pagar|Bienestar financiero|PROT-07|Bienestar financiero|Bienestar financiero|Bienestar biopsicosocial##
## |1_estres|A_y_B|Estrés - individual|Respuestas físicas de estrés|Dolores en el cuello y espalda o tensión muscular|Somatización y fatiga física|PROT-08|Bienestar físico y hábitos saludables|Bienestar físico|Bienestar biopsicosocial##
## |2_estres|A_y_B|Estrés - individual|Respuestas físicas de estrés|Problemas gastrointestinales, ulcera péptica, acidez, problemas digestivos o de colon.|Somatización y fatiga física|PROT-08|Bienestar físico y hábitos saludables|Bienestar físico|Bienestar biopsicosocial##
## |3_estres|A_y_B|Estrés - individual|Respuestas físicas de estrés|Problemas respiratorios|Somatización y fatiga física|PROT-08|Bienestar físico y hábitos saludables|Bienestar físico|Bienestar biopsicosocial##
## |4_estres|A_y_B|Estrés - individual|Respuestas físicas de estrés|Dolor de cabeza|Somatización y fatiga física|PROT-08|Bienestar físico y hábitos saludables|Bienestar físico|Bienestar biopsicosocial##
## |5_estres|A_y_B|Estrés - individual|Respuestas físicas de estrés|Trastornos del sueño como somnolencia durante el día o desvelo en la noche|Somatización y fatiga física|PROT-08|Bienestar físico y hábitos saludables|Bienestar físico|Bienestar biopsicosocial##
## |6_estres|A_y_B|Estrés - individual|Respuestas físicas de estrés|Palpitaciones en el pecho o problemas cardiacos|Somatización y fatiga física|PROT-08|Bienestar físico y hábitos saludables|Bienestar físico|Bienestar biopsicosocial##
## |7_estres|A_y_B|Estrés - individual|Respuestas físicas de estrés|Cambios fuertes de apetito|Somatización y fatiga física|PROT-08|Bienestar físico y hábitos saludables|Bienestar físico|Bienestar biopsicosocial##
## |8_estres|A_y_B|Estrés - individual|Respuestas físicas de estrés|Problemas relacionados con la función de los órganos genitales (impotencia, frigidez)|Somatización y fatiga física|PROT-08|Bienestar físico y hábitos saludables|Bienestar físico|Bienestar biopsicosocial##
## |18_extra|A_y_B|Extralaboral|Comunicación y relaciones interpersonales|Tengo buena comunicación con las personas cercanas|Apoyo social|PROT-06|Bienestar extralaboral|Bienestar extralaboral|Bienestar biopsicosocial##
## |19_extra|A_y_B|Extralaboral|Comunicación y relaciones interpersonales|Las relaciones con mis amigos son buenas|Apoyo social|PROT-06|Bienestar extralaboral|Bienestar extralaboral|Bienestar biopsicosocial##
## |20_extra|A_y_B|Extralaboral|Comunicación y relaciones interpersonales|Converso con personas cercanas sobre diferentes temas|Apoyo social|PROT-06|Bienestar extralaboral|Bienestar extralaboral|Bienestar biopsicosocial##
## |21_extra|A_y_B|Extralaboral|Comunicación y relaciones interpersonales|Mis amigos están dispuestos a escucharme cuando tengo problemas|Apoyo social|PROT-06|Bienestar extralaboral|Bienestar extralaboral|Bienestar biopsicosocial##
## |23_extra|A_y_B|Extralaboral|Comunicación y relaciones interpersonales|Puedo hablar con personas cercanas sobre las cosas que me pasan|Apoyo social|PROT-06|Bienestar extralaboral|Bienestar extralaboral|Bienestar biopsicosocial##
## |11_estres|A_y_B|Extralaboral|Relaciones familiares|Dificultad en las relaciones con otras personas|Dererioro de relaciones sociales y aislamiento|PROT-06|Bienestar extralaboral|Bienestar extralaboral|Bienestar biopsicosocial##
## |9_estres|A_y_B|Extralaboral|Relaciones familiares|Dificultad en las relaciones familiares|Dererioro de relaciones sociales y aislamiento|PROT-06|Bienestar extralaboral|Bienestar extralaboral|Bienestar biopsicosocial##
## |22_extra|A_y_B|Extralaboral|Relaciones familiares|Cuento con el apoyo de mi familia cuando tengo problemas|Relación con el núcleo familiar|PROT-06|Bienestar extralaboral|Bienestar extralaboral|Bienestar biopsicosocial##
## |25_extra|A_y_B|Extralaboral|Relaciones familiares|La relación con mi familia cercana es cordial|Relación con el núcleo familiar|PROT-06|Bienestar extralaboral|Bienestar extralaboral|Bienestar biopsicosocial##
## |27_extra|A_y_B|Extralaboral|Relaciones familiares|Los problemas con mis familiares los resolvemos de forma amistosa|Relación con el núcleo familiar|PROT-06|Bienestar extralaboral|Bienestar extralaboral|Bienestar biopsicosocial##
## |13_estres|A_y_B|Estrés - individual|Respuestas comportamentales de estrés|Sentimiento de sobrecarga de trabajo|Desmotivación y desgaste laboral|PROT-20|Gestión del Engagement organizacional|Motivación laboral|Bienestar biopsicosocial##
## |17_estres|A_y_B|Estrés - individual|Respuestas comportamentales de estrés|Cansancio, tedio o desgano|Desmotivación y desgaste laboral|PROT-20|Gestión del Engagement organizacional|Motivación laboral|Bienestar biopsicosocial##
## |18_estres|A_y_B|Estrés - individual|Respuestas comportamentales de estrés|Disminución del rendimiento en el trabajo o poca creatividad|Desmotivación y desgaste laboral|PROT-20|Gestión del Engagement organizacional|Motivación laboral|Bienestar biopsicosocial##
## |19_estres|A_y_B|Estrés - individual|Respuestas comportamentales de estrés|Deseo de no asistir al trabajo|Desmotivación y desgaste laboral|PROT-20|Gestión del Engagement organizacional|Motivación laboral|Bienestar biopsicosocial##
## |20_estres|A_y_B|Estrés - individual|Respuestas comportamentales de estrés|Bajo compromiso o poco interés con lo que se hace|Desmotivación y desgaste laboral|PROT-20|Gestión del Engagement organizacional|Motivación laboral|Bienestar biopsicosocial##
## |22_estres|A_y_B|Estrés - individual|Respuestas comportamentales de estrés|Deseo de cambiar de empleo|Desmotivación y desgaste laboral|PROT-20|Gestión del Engagement organizacional|Motivación laboral|Bienestar biopsicosocial##
## |12_estres|A_y_B|Estrés - individual|Respuestas emocionales de estrés|Sensación de aislamiento y desinterés|Pérdida de sentido|PROT-03|Bienestar emocional y trascendente — Promoción salud mental positiva|Bienestar emocional y trascendente|Bienestar biopsicosocial##
## |16_estres|A_y_B|Estrés - individual|Respuestas emocionales de estrés|Sentimiento de frustración, de no haber hecho lo que se quería en la vida|Pérdida de sentido|PROT-03|Bienestar emocional y trascendente — Promoción salud mental positiva|Bienestar emocional y trascendente|Bienestar biopsicosocial##
## |27_estres|A_y_B|Estrés - individual|Respuestas emocionales de estrés|Sentimiento de que “no vale nada “o “no sirve para nada”.|Pérdida de sentido|PROT-03|Bienestar emocional y trascendente — Promoción salud mental positiva|Bienestar emocional y trascendente|Bienestar biopsicosocial##
## |24_extra|A_y_B|Extralaboral|Influencia del entorno extralaboral sobre el trabajo|Mis problemas personales o familiares afectan mi trabajo|Conflicto vida - trabajo|PROT-09|Bienestar vida-trabajo (conciliación trabajo-familia)|Bienestar vida-trabajo|Bienestar biopsicosocial##
## |26_extra|A_y_B|Extralaboral|Influencia del entorno extralaboral sobre el trabajo|Mis problemas personales o familiares me quitan la energía que necesito para trabajar|Conflicto vida - trabajo|PROT-09|Bienestar vida-trabajo (conciliación trabajo-familia)|Bienestar vida-trabajo|Bienestar biopsicosocial##
## |28_extra|A_y_B|Extralaboral|Influencia del entorno extralaboral sobre el trabajo|Mis problemas personales o familiares afectan mis relaciones en el trabajo|Conflicto vida - trabajo|PROT-09|Bienestar vida-trabajo (conciliación trabajo-familia)|Bienestar vida-trabajo|Bienestar biopsicosocial##
## |14_extra|A_y_B|Extralaboral|Balance entre la vida laboral y familiar|Tengo tiempo para actividades de esparcimiento|Equilibrio y recuperación|PROT-09|Bienestar vida-trabajo (conciliación trabajo-familia)|Bienestar vida-trabajo|Bienestar biopsicosocial##
## |15_extra|A_y_B|Extralaboral|Balance entre la vida laboral y familiar|Tengo tiempo suficiente para descansar|Equilibrio y recuperación|PROT-09|Bienestar vida-trabajo (conciliación trabajo-familia)|Bienestar vida-trabajo|Bienestar biopsicosocial##
## |16_extra|A_y_B|Extralaboral|Balance entre la vida laboral y familiar|Mi trabajo me permite tener disponibilidad para atender compromisos personales y familiares|Equilibrio y recuperación|PROT-09|Bienestar vida-trabajo (conciliación trabajo-familia)|Bienestar vida-trabajo|Bienestar biopsicosocial##
## |17_extra|A_y_B|Extralaboral|Balance entre la vida laboral y familiar|Tengo tiempo para compartir con mi familia o amigos|Equilibrio y recuperación|PROT-09|Bienestar vida-trabajo (conciliación trabajo-familia)|Bienestar vida-trabajo|Bienestar biopsicosocial##
## |35_intra|A|Intralaboral|Influencia del trabajo sobre el entorno extra|Cuando estoy en casa sigo pensando en el trabajo|Impacto cognitivo-relacional|PROT-09|Bienestar vida-trabajo (conciliación trabajo-familia)|Bienestar vida-trabajo|Bienestar biopsicosocial##
## |36_intra|A|Intralaboral|Influencia del trabajo sobre el entorno extra|Discuto con mi familia y amigos por causa de mi trabajo|Impacto cognitivo-relacional|PROT-09|Bienestar vida-trabajo (conciliación trabajo-familia)|Bienestar vida-trabajo|Bienestar biopsicosocial##
## |37_intra|A|Intralaboral|Influencia del trabajo sobre el entorno extra|Debo atender asuntos de trabajo cuando estoy en casa|Interferencia temporal|PROT-09|Bienestar vida-trabajo (conciliación trabajo-familia)|Bienestar vida-trabajo|Bienestar biopsicosocial##
## |38_intra|A|Intralaboral|Influencia del trabajo sobre el entorno extra|Por mi trabajo el tiempo que paso con mi familia y amigos es muy poco|Interferencia temporal|PROT-09|Bienestar vida-trabajo (conciliación trabajo-familia)|Bienestar vida-trabajo|Bienestar biopsicosocial##
## |25_intra|B|Intralaboral|Influencia del trabajo sobre el entorno extra|Cuando estoy en casa sigo pensando en el trabajo|Impacto cognitivo-relacional|PROT-09|Bienestar vida-trabajo (conciliación trabajo-familia)|Bienestar vida-trabajo|Bienestar biopsicosocial##
## |26_intra|B|Intralaboral|Influencia del trabajo sobre el entorno extra|Discuto con mi familia o amigos por causa de mi trabajo|Impacto cognitivo-relacional|PROT-09|Bienestar vida-trabajo (conciliación trabajo-familia)|Bienestar vida-trabajo|Bienestar biopsicosocial##
## |27_intra|B|Intralaboral|Influencia del trabajo sobre el entorno extra|Debo atender asuntos de trabajo cuando estoy en casa|Interferencia temporal|PROT-09|Bienestar vida-trabajo (conciliación trabajo-familia)|Bienestar vida-trabajo|Bienestar biopsicosocial##
## |28_intra|B|Intralaboral|Influencia del trabajo sobre el entorno extra|Por mi trabajo el tiempo que paso con mi familia y amigos es muy poco|Interferencia temporal|PROT-09|Bienestar vida-trabajo (conciliación trabajo-familia)|Bienestar vida-trabajo|Bienestar biopsicosocial##
## |1_extra|A_y_B|Extralaboral|Desplazamiento vivienda trabajo vivienda|Es fácil transportarme entre mi casa y mi trabajo|Movilidad eficiente y desplazamiento saludable|PROT-06|Bienestar extralaboral|Bienestar extralaboral|Bienestar biopsicosocial##
## |2_extra|A_y_B|Extralaboral|Desplazamiento vivienda trabajo vivienda|Tengo que tomar varios medios de transporte para llegar a mi trabajo|Movilidad eficiente y desplazamiento saludable|PROT-06|Bienestar extralaboral|Bienestar extralaboral|Bienestar biopsicosocial##
## |3_extra|A_y_B|Extralaboral|Desplazamiento vivienda trabajo vivienda|Paso mucho tiempo viajando de ida y regreso al trabajo|Movilidad eficiente y desplazamiento saludable|PROT-06|Bienestar extralaboral|Bienestar extralaboral|Bienestar biopsicosocial##
## |4_extra|A_y_B|Extralaboral|Desplazamiento vivienda trabajo vivienda|Me transporto cómodamente entre mi casa y el trabajo|Movilidad eficiente y desplazamiento saludable|PROT-06|Bienestar extralaboral|Bienestar extralaboral|Bienestar biopsicosocial##
## |15_estres|A_y_B|Estrés - individual|Respuestas comportamentales de estrés|Aumento en el número de accidentes de trabajo|Conductas de riesgo|PROT-10|Prevención de conductas de riesgo|Comportamientos seguros|Bienestar biopsicosocial##
## |26_estres|A_y_B|Estrés - individual|Respuestas comportamentales de estrés|Consumo de drogas para aliviar la tensión o los nervios|Conductas de riesgo|PROT-10|Prevención de conductas de riesgo|Comportamientos seguros|Bienestar biopsicosocial##
## |28_estres|A_y_B|Estrés - individual|Respuestas comportamentales de estrés|Consumo de bebidas alcohólicas o café o cigarrillo.|Conductas de riesgo|PROT-10|Prevención de conductas de riesgo|Comportamientos seguros|Bienestar biopsicosocial##
## |30_estres|A_y_B|Estrés - individual|Respuestas comportamentales de estrés|Comportamientos rígidos, obstinación o terquedad.|Conductas de riesgo|PROT-10|Prevención de conductas de riesgo|Comportamientos seguros|Bienestar biopsicosocial##
## |7_extra|A_y_B|Extralaboral|Características de la vivienda y de su entorno|Desde donde vivo me es fácil llegar al centro médico donde me atienden|Accesibilidad del entorno|PROT-06|Bienestar extralaboral|Bienestar extralaboral|Bienestar biopsicosocial##
## |8_extra|A_y_B|Extralaboral|Características de la vivienda y de su entorno|Cerca de mi vivienda las vías están en buenas condiciones|Accesibilidad del entorno|PROT-06|Bienestar extralaboral|Bienestar extralaboral|Bienestar biopsicosocial##
## |9_extra|A_y_B|Extralaboral|Características de la vivienda y de su entorno|Cerca de mi vivienda encuentro fácilmente transporte|Accesibilidad del entorno|PROT-06|Bienestar extralaboral|Bienestar extralaboral|Bienestar biopsicosocial##
## |5_extra|A_y_B|Extralaboral|Características de la vivienda y de su entorno|La zona donde vivo es segura|Seguridad del entorno|PROT-06|Bienestar extralaboral|Bienestar extralaboral|Bienestar biopsicosocial##
## |6_extra|A_y_B|Extralaboral|Características de la vivienda y de su entorno|En la zona donde vivo se presentan hurtos y mucha delincuencia|Seguridad del entorno|PROT-06|Bienestar extralaboral|Bienestar extralaboral|Bienestar biopsicosocial##
## |10_extra|A_y_B|Extralaboral|Características de la vivienda y de su entorno|Las condiciones de mi vivienda son buenas|Condiciones de la vivienda|PROT-06|Bienestar extralaboral|Bienestar extralaboral|Bienestar biopsicosocial##
## |11_extra|A_y_B|Extralaboral|Características de la vivienda y de su entorno|En mi vivienda hay servicio de agua y luz|Condiciones de la vivienda|PROT-06|Bienestar extralaboral|Bienestar extralaboral|Bienestar biopsicosocial##
## |12_extra|A_y_B|Extralaboral|Características de la vivienda y de su entorno|Las condiciones de mi vivienda me permiten descansar cuando lo quiero|Condiciones de la vivienda|PROT-06|Bienestar extralaboral|Bienestar extralaboral|Bienestar biopsicosocial##
## |13_extra|A_y_B|Extralaboral|Características de la vivienda y de su entorno|Las condiciones de mi vivienda me permiten sentirme cómodo|Condiciones de la vivienda|PROT-06|Bienestar extralaboral|Bienestar extralaboral|Bienestar biopsicosocial##
## |9_capitalpsicologico|A_y_B|Estrés - individual|Resiliencia|Considero los obstáculos como retos que puedo superar|Actitud resiliente|PROT-01|Afrontamiento del estrés y recursos psicológicos|Afrontamiento del estrés y recursos psicológicos|Bienestar biopsicosocial##
## |7_capitalpsicologico|A_y_B|Estrés - individual|Resiliencia|Las experiencias difíciles de mi vida me han llevado a hacer cosas que no habia pensado ser capaz de hacer|Aprendizaje resiliente|PROT-01|Afrontamiento del estrés y recursos psicológicos|Afrontamiento del estrés y recursos psicológicos|Bienestar biopsicosocial##
## |10_capitalpsicologico|A_y_B|Estrés - individual|Autoeficacia|Estoy seguro de que puedo manejar situaciones difíciles|Autoeficacia percibida|PROT-01|Afrontamiento del estrés y recursos psicológicos|Afrontamiento del estrés y recursos psicológicos|Bienestar biopsicosocial##
## |5_capitalpsicologico|A_y_B|Estrés - individual|Esperanza|Es posible alcanzar las metas que me he propuesto en la vida|Convicción por las metas|PROT-01|Afrontamiento del estrés y recursos psicológicos|Afrontamiento del estrés y recursos psicológicos|Bienestar biopsicosocial##
## |6_capitalpsicologico|A_y_B|Estrés - individual|Esperanza|Creo que con esfuerzo y dedicación se pueden lograr los objetivos|Esfuerzo y dedicación|PROT-01|Afrontamiento del estrés y recursos psicológicos|Afrontamiento del estrés y recursos psicológicos|Bienestar biopsicosocial##
## |4_capitalpsicologico|A_y_B|Estrés - individual|Esperanza|Aún en momentos de dificultad creo que es posible un futuro mejor|Esperanza del futuro|PROT-01|Afrontamiento del estrés y recursos psicológicos|Afrontamiento del estrés y recursos psicológicos|Bienestar biopsicosocial##
## |1_capitalpsicologico|A_y_B|Estrés - individual|Optimismo|Me mantengo firme en la lucha por alcanzar mis metas|Firmeza y perseverancia|PROT-01|Afrontamiento del estrés y recursos psicológicos|Afrontamiento del estrés y recursos psicológicos|Bienestar biopsicosocial##
## |12_capitalpsicologico|A_y_B|Estrés - individual|Autoeficacia|Las cosas buenas que pasen dependen de mis acciones|Locus de control interno|PROT-01|Afrontamiento del estrés y recursos psicológicos|Afrontamiento del estrés y recursos psicológicos|Bienestar biopsicosocial##
## |2_capitalpsicologico|A_y_B|Estrés - individual|Optimismo|Estoy convencido(a) que me esperan cosas buenas en la vida|Optimismo del porvenir|PROT-01|Afrontamiento del estrés y recursos psicológicos|Afrontamiento del estrés y recursos psicológicos|Bienestar biopsicosocial##
## |3_capitalpsicologico|A_y_B|Estrés - individual|Optimismo|Cuando un plan me falla busco otras opciones que funcionen para cumplir el objetivo|Resolutividad|PROT-01|Afrontamiento del estrés y recursos psicológicos|Afrontamiento del estrés y recursos psicológicos|Bienestar biopsicosocial##
## |11_capitalpsicologico|A_y_B|Estrés - individual|Autoeficacia|A menudo siento que soy capaz de hacer lo que me propongo|Sensación de logro|PROT-01|Afrontamiento del estrés y recursos psicológicos|Afrontamiento del estrés y recursos psicológicos|Bienestar biopsicosocial##
## |8_capitalpsicologico|A_y_B|Estrés - individual|Resiliencia|Puedo recuperarme rápidamente después de experimentar dificultades|Superación de adversidades|PROT-01|Afrontamiento del estrés y recursos psicológicos|Afrontamiento del estrés y recursos psicológicos|Bienestar biopsicosocial##
## |54_intra|A|Intralaboral|Claridad de rol|Me informan cuáles son las decisiones que puedo tomar en mi trabajo|Autonomía decisional|PROT-11|Arquitectura de roles y responsabilidades|Arquitectura de roles y responsabilidades|Condiciones de trabajo saludable##
## |53_intra|A|Intralaboral|Claridad de rol|Me informan con claridad cuáles son mis funciones|Claridad en funciones y objetivos del rol|PROT-11|Arquitectura de roles y responsabilidades|Arquitectura de roles y responsabilidades|Condiciones de trabajo saludable##
## |55_intra|A|Intralaboral|Claridad de rol|Me explican claramente los resultados que debo lograr en mi trabajo|Claridad en funciones y objetivos del rol|PROT-11|Arquitectura de roles y responsabilidades|Arquitectura de roles y responsabilidades|Condiciones de trabajo saludable##
## |57_intra|A|Intralaboral|Claridad de rol|Me explican claramente los objetivos de mi trabajo|Claridad en funciones y objetivos del rol|PROT-11|Arquitectura de roles y responsabilidades|Arquitectura de roles y responsabilidades|Condiciones de trabajo saludable##
## |19_intra|A|Intralaboral|Exigencias de responsabilidad del cargo|En mi trabajo tengo que tomar decisiones difíciles muy rápido|Decisiones críticas y resultados de gestión|PROT-11|Arquitectura de roles y responsabilidades|Arquitectura de roles y responsabilidades|Condiciones de trabajo saludable##
## |25_intra|A|Intralaboral|Exigencias de responsabilidad del cargo|Respondo ante mi jefe por los resultados de toda mi área de trabajo|Decisiones críticas y resultados de gestión|PROT-11|Arquitectura de roles y responsabilidades|Arquitectura de roles y responsabilidades|Condiciones de trabajo saludable##
## |29_intra|A|Intralaboral|Consistencia del rol|En mi trabajo se presentan situaciones en las que debo pasar por alto normas o procedimientos|Integridad normativa|PROT-11|Arquitectura de roles y responsabilidades|Arquitectura de roles y responsabilidades|Condiciones de trabajo saludable##
## |28_intra|A|Intralaboral|Consistencia del rol|En mi trabajo me piden hacer cosas innecesarias|Pertinencia operativa|PROT-11|Arquitectura de roles y responsabilidades|Arquitectura de roles y responsabilidades|Condiciones de trabajo saludable##
## |30_intra|A|Intralaboral|Consistencia del rol|En mi trabajo tengo que hacer cosas que se podrían hacer de una forma más practica|Pertinencia operativa|PROT-11|Arquitectura de roles y responsabilidades|Arquitectura de roles y responsabilidades|Condiciones de trabajo saludable##
## |56_intra|A|Intralaboral|Claridad de rol|Me explican claramente el efecto de mi trabajo en la empresa|Propósito del rol y redes de soporte|PROT-11|Arquitectura de roles y responsabilidades|Arquitectura de roles y responsabilidades|Condiciones de trabajo saludable##
## |58_intra|A|Intralaboral|Claridad de rol|Me informan claramente quien me puede orientar para hacer mi trabajo|Propósito del rol y redes de soporte|PROT-11|Arquitectura de roles y responsabilidades|Arquitectura de roles y responsabilidades|Condiciones de trabajo saludable##
## |59_intra|A|Intralaboral|Claridad de rol|Me informan claramente con quien puedo resolver los asuntos de trabajo|Propósito del rol y redes de soporte|PROT-11|Arquitectura de roles y responsabilidades|Arquitectura de roles y responsabilidades|Condiciones de trabajo saludable##
## |22_intra|A|Intralaboral|Exigencias de responsabilidad del cargo|En mi trabajo respondo por cosas de mucho valor|Responsabilidad por bienes y valores|PROT-11|Arquitectura de roles y responsabilidades|Arquitectura de roles y responsabilidades|Condiciones de trabajo saludable##
## |23_intra|A|Intralaboral|Exigencias de responsabilidad del cargo|En mi trabajo respondo por dinero de la empresa|Responsabilidad por bienes y valores|PROT-11|Arquitectura de roles y responsabilidades|Arquitectura de roles y responsabilidades|Condiciones de trabajo saludable##
## |24_intra|A|Intralaboral|Exigencias de responsabilidad del cargo|Como parte de mis funciones debo responder por la seguridad de otros|Responsabilidad por la integridad y salud de otros|PROT-11|Arquitectura de roles y responsabilidades|Arquitectura de roles y responsabilidades|Condiciones de trabajo saludable##
## |26_intra|A|Intralaboral|Exigencias de responsabilidad del cargo|Mi trabajo me exige cuidar la salud de otras personas|Responsabilidad por la integridad y salud de otros|PROT-11|Arquitectura de roles y responsabilidades|Arquitectura de roles y responsabilidades|Condiciones de trabajo saludable##
## |27_intra|A|Intralaboral|Consistencia del rol|En el trabajo me dan ordenes contradictorias|Contradicción en estructura de mando|PROT-11|Arquitectura de roles y responsabilidades|Arquitectura de roles y responsabilidades|Condiciones de trabajo saludable##
## |42_intra|B|Intralaboral|Claridad de rol|Me informan cuáles son las decisiones que puedo tomar en mi trabajo|Autonomía decisional|PROT-11|Arquitectura de roles y responsabilidades|Arquitectura de roles y responsabilidades|Condiciones de trabajo saludable##
## |41_intra|B|Intralaboral|Claridad de rol|Me informan con claridad cuáles son mis funciones|Claridad en funciones y objetivos del rol|PROT-11|Arquitectura de roles y responsabilidades|Arquitectura de roles y responsabilidades|Condiciones de trabajo saludable##
## |43_intra|B|Intralaboral|Claridad de rol|Me explican claramente los resultados que debo lograr en mi trabajo|Claridad en funciones y objetivos del rol|PROT-11|Arquitectura de roles y responsabilidades|Arquitectura de roles y responsabilidades|Condiciones de trabajo saludable##
## |44_intra|B|Intralaboral|Claridad de rol|Me explican claramente los objetivos de mi trabajo|Claridad en funciones y objetivos del rol|PROT-11|Arquitectura de roles y responsabilidades|Arquitectura de roles y responsabilidades|Condiciones de trabajo saludable##
## |45_intra|B|Intralaboral|Claridad de rol|Me informan claramente con quien puedo resolver los asuntos de trabajo|Propósito del rol y redes de soporte|PROT-11|Arquitectura de roles y responsabilidades|Arquitectura de roles y responsabilidades|Condiciones de trabajo saludable##
## |60_intra|A|Intralaboral|Capacitación|La empresa me permite asistir a capacitaciones relacionadas con mi trabajo|Acceso a capacitación|PROT-12|Estrategia de aprendizaje y desarrollo (L&D Strategy)|Aprendizaje y desarrollo (L&D Strategy)|Condiciones de trabajo saludable##
## |40_intra|A|Intralaboral|Oportunidades de desarrollo y uso de habilidad|Mi trabajo me permite aplicar mis conocimientos|Alineación de competencias y ajuste al cargo|PROT-12|Estrategia de aprendizaje y desarrollo (L&D Strategy)|Aprendizaje y desarrollo (L&D Strategy)|Condiciones de trabajo saludable##
## |42_intra|A|Intralaboral|Oportunidades de desarrollo y uso de habilidad|Me asignan el trabajo teniendo en cuenta mis capacidades|Alineación de competencias y ajuste al cargo|PROT-12|Estrategia de aprendizaje y desarrollo (L&D Strategy)|Aprendizaje y desarrollo (L&D Strategy)|Condiciones de trabajo saludable##
## |39_intra|A|Intralaboral|Oportunidades de desarrollo y uso de habilidad|Mi trabajo me permite desarrollar mis habilidades|Rol enriquecido|PROT-12|Estrategia de aprendizaje y desarrollo (L&D Strategy)|Aprendizaje y desarrollo (L&D Strategy)|Condiciones de trabajo saludable##
## |41_intra|A|Intralaboral|Oportunidades de desarrollo y uso de habilidad|Mi trabajo me permite aprender nuevas cosas|Rol enriquecido|PROT-12|Estrategia de aprendizaje y desarrollo (L&D Strategy)|Aprendizaje y desarrollo (L&D Strategy)|Condiciones de trabajo saludable##
## |61_intra|A|Intralaboral|Capacitación|Recibo capacitación útil para hacer mi trabajo|Pertinencia y aplicabilidad de la capacitación|PROT-12|Estrategia de aprendizaje y desarrollo (L&D Strategy)|Aprendizaje y desarrollo (L&D Strategy)|Condiciones de trabajo saludable##
## |62_intra|A|Intralaboral|Capacitación|Recibo capacitación que me ayuda a hacer mejor mi trabajo|Pertinencia y aplicabilidad de la capacitación|PROT-12|Estrategia de aprendizaje y desarrollo (L&D Strategy)|Aprendizaje y desarrollo (L&D Strategy)|Condiciones de trabajo saludable##
## |46_intra|B|Intralaboral|Capacitación|La empresa me permite asistir a capacitaciones relacionadas con trabajo|Acceso a capacitación|PROT-12|Estrategia de aprendizaje y desarrollo (L&D Strategy)|Aprendizaje y desarrollo (L&D Strategy)|Condiciones de trabajo saludable##
## |31_intra|B|Intralaboral|Oportunidades de desarrollo y uso de habilidad|Mi trabajo me permite aplicar mis conocimientos|Alineación de competencias y ajuste al cargo|PROT-12|Estrategia de aprendizaje y desarrollo (L&D Strategy)|Aprendizaje y desarrollo (L&D Strategy)|Condiciones de trabajo saludable##
## |29_intra|B|Intralaboral|Oportunidades de desarrollo y uso de habilidad|En mi trabajo puedo hacer cosas nuevas|Rol enriquecido|PROT-12|Estrategia de aprendizaje y desarrollo (L&D Strategy)|Aprendizaje y desarrollo (L&D Strategy)|Condiciones de trabajo saludable##
## |30_intra|B|Intralaboral|Oportunidades de desarrollo y uso de habilidad|Mi trabajo me permite desarrollar mis habilidades|Rol enriquecido|PROT-12|Estrategia de aprendizaje y desarrollo (L&D Strategy)|Aprendizaje y desarrollo (L&D Strategy)|Condiciones de trabajo saludable##
## |32_intra|B|Intralaboral|Oportunidades de desarrollo y uso de habilidad|Mi trabajo me permite aprender nuevas cosas|Rol enriquecido|PROT-12|Estrategia de aprendizaje y desarrollo (L&D Strategy)|Aprendizaje y desarrollo (L&D Strategy)|Condiciones de trabajo saludable##
## |47_intra|B|Intralaboral|Capacitación|Recibo capacitación útil para hacer mi trabajo|Pertinencia y aplicabilidad de la capacitación|PROT-12|Estrategia de aprendizaje y desarrollo (L&D Strategy)|Aprendizaje y desarrollo (L&D Strategy)|Condiciones de trabajo saludable##
## |48_intra|B|Intralaboral|Capacitación|Recibo capacitación que me ayuda hacer mejor mi trabajo|Pertinencia y aplicabilidad de la capacitación|PROT-12|Estrategia de aprendizaje y desarrollo (L&D Strategy)|Aprendizaje y desarrollo (L&D Strategy)|Condiciones de trabajo saludable##
## |113_intra|A|Intralaboral|Demandas emocionales|Para hacer mi trabajo debo mostrar sentimientos distintos a los míos|Disonancia emocional|PROT-13|Gestión de condiciones emocionales — Demandas emocionales|Condiciones emocionales saludables|Condiciones de trabajo saludable##
## |108_intra|A|Intralaboral|Demandas emocionales|Atiendo clientes o usuarios muy preocupados|Exposición al sufrimiento|PROT-13|Gestión de condiciones emocionales — Demandas emocionales|Condiciones emocionales saludables|Condiciones de trabajo saludable##
## |109_intra|A|Intralaboral|Demandas emocionales|Atiendo clientes o usuarios muy tristes|Exposición al sufrimiento|PROT-13|Gestión de condiciones emocionales — Demandas emocionales|Condiciones emocionales saludables|Condiciones de trabajo saludable##
## |110_intra|A|Intralaboral|Demandas emocionales|Mi trabajo me exige atender personas muy enfermas|Exposición al sufrimiento|PROT-13|Gestión de condiciones emocionales — Demandas emocionales|Condiciones emocionales saludables|Condiciones de trabajo saludable##
## |111_intra|A|Intralaboral|Demandas emocionales|Mi trabajo me exige atender personas muy necesitadas de ayuda|Exposición al sufrimiento|PROT-13|Gestión de condiciones emocionales — Demandas emocionales|Condiciones emocionales saludables|Condiciones de trabajo saludable##
## |115_intra|A|Intralaboral|Demandas emocionales|Mi trabajo me exige atender situaciones muy tristes o dolorosas|Exposición al sufrimiento|PROT-13|Gestión de condiciones emocionales — Demandas emocionales|Condiciones emocionales saludables|Condiciones de trabajo saludable##
## |107_intra|A|Intralaboral|Demandas emocionales|Atiendo clientes o usuarios muy enojados|Hostilidad y violencia|PROT-18|Convivencia, relacionamiento y prevención de violencia|Condiciones emocionales saludables|Condiciones de trabajo saludable##
## |112_intra|A|Intralaboral|Demandas emocionales|Atiendo clientes o usuarios que me maltratan|Hostilidad y violencia|PROT-18|Convivencia, relacionamiento y prevención de violencia|Condiciones emocionales saludables|Condiciones de trabajo saludable##
## |114_intra|A|Intralaboral|Demandas emocionales|Mi trabajo me exige atender situaciones de violencia|Hostilidad y violencia|PROT-18|Convivencia, relacionamiento y prevención de violencia|Condiciones emocionales saludables|Condiciones de trabajo saludable##
## |106_intra|A|Intralaboral|Demandas emocionales|En mi trabajo debo brindar servicio a clientes o usuarios|Disonancia emocional|PROT-13|Gestión de condiciones emocionales — Demandas emocionales|Condiciones emocionales saludables|Condiciones de trabajo saludable##
## |98_intra|B|Intralaboral|Demandas emocionales|Puedo expresar tristeza o enojo frente a las personas que atiendo|Disonancia emocional|PROT-13|Gestión de condiciones emocionales — Demandas emocionales|Condiciones emocionales saludables|Condiciones de trabajo saludable##
## |91_intra|B|Intralaboral|Demandas emocionales|Atiendo clientes o usuarios muy preocupados|Exposición al sufrimiento|PROT-13|Gestión de condiciones emocionales — Demandas emocionales|Condiciones emocionales saludables|Condiciones de trabajo saludable##
## |92_intra|B|Intralaboral|Demandas emocionales|Atiendo clientes o usuarios muy tristes|Exposición al sufrimiento|PROT-13|Gestión de condiciones emocionales — Demandas emocionales|Condiciones emocionales saludables|Condiciones de trabajo saludable##
## |93_intra|B|Intralaboral|Demandas emocionales|Mi trabajo me exige atender personas muy enfermas|Exposición al sufrimiento|PROT-13|Gestión de condiciones emocionales — Demandas emocionales|Condiciones emocionales saludables|Condiciones de trabajo saludable##
## |94_intra|B|Intralaboral|Demandas emocionales|Mi trabajo me exige atender personas muy necesitadas de ayuda|Exposición al sufrimiento|PROT-13|Gestión de condiciones emocionales — Demandas emocionales|Condiciones emocionales saludables|Condiciones de trabajo saludable##
## |97_intra|B|Intralaboral|Demandas emocionales|Mi trabajo me exige atender situaciones muy tristes o dolorosas|Exposición al sufrimiento|PROT-13|Gestión de condiciones emocionales — Demandas emocionales|Condiciones emocionales saludables|Condiciones de trabajo saludable##
## |90_intra|B|Intralaboral|Demandas emocionales|Atiendo clientes o usuarios muy enojados|Hostilidad y violencia|PROT-18|Convivencia, relacionamiento y prevención de violencia|Condiciones emocionales saludables|Condiciones de trabajo saludable##
## |95_intra|B|Intralaboral|Demandas emocionales|Atiendo clientes o usuarios que me maltratan|Hostilidad y violencia|PROT-18|Convivencia, relacionamiento y prevención de violencia|Condiciones emocionales saludables|Condiciones de trabajo saludable##
## |96_intra|B|Intralaboral|Demandas emocionales|Mi trabajo me exige atender situaciones de violencia|Hostilidad y violencia|PROT-18|Convivencia, relacionamiento y prevención de violencia|Condiciones emocionales saludables|Condiciones de trabajo saludable##
## |89_intra|B|Intralaboral|Demandas emocionales|En mi trabajo debo brindar servicio a clientes o usuarios|Disonancia emocional|PROT-13|Gestión de condiciones emocionales — Demandas emocionales|Condiciones emocionales saludables|Condiciones de trabajo saludable##
## |1_intra|A|Intralaboral|Demandas ambientales y de esfuerzo físico|El ruido en el lugar donde trabajo es molesto|Confort del entorno|PROT-14|Gestión de condiciones físicas y ergonomía|Condiciones físicas saludables|Condiciones de trabajo saludable##
## |2_intra|A|Intralaboral|Demandas ambientales y de esfuerzo físico|En el lugar donde trabajo hace mucho frio|Confort del entorno|PROT-14|Gestión de condiciones físicas y ergonomía|Condiciones físicas saludables|Condiciones de trabajo saludable##
## |3_intra|A|Intralaboral|Demandas ambientales y de esfuerzo físico|En el lugar donde trabajo hace mucho calor|Confort del entorno|PROT-14|Gestión de condiciones físicas y ergonomía|Condiciones físicas saludables|Condiciones de trabajo saludable##
## |4_intra|A|Intralaboral|Demandas ambientales y de esfuerzo físico|El aire en el lugar donde trabajo es fresco y agradable|Confort del entorno|PROT-14|Gestión de condiciones físicas y ergonomía|Condiciones físicas saludables|Condiciones de trabajo saludable##
## |5_intra|A|Intralaboral|Demandas ambientales y de esfuerzo físico|La luz del sitio donde trabajo es agradable|Confort del entorno|PROT-14|Gestión de condiciones físicas y ergonomía|Condiciones físicas saludables|Condiciones de trabajo saludable##
## |6_intra|A|Intralaboral|Demandas ambientales y de esfuerzo físico|El espacio donde trabajo es cómodo|Confort del entorno|PROT-14|Gestión de condiciones físicas y ergonomía|Condiciones físicas saludables|Condiciones de trabajo saludable##
## |12_intra|A|Intralaboral|Demandas ambientales y de esfuerzo físico|El lugar donde trabajo es limpio y ordenado|Orden percibido|PROT-14|Gestión de condiciones físicas y ergonomía|Condiciones físicas saludables|Condiciones de trabajo saludable##
## |10_intra|A|Intralaboral|Demandas ambientales y de esfuerzo físico|En mi trabajo me preocupa estar expuesto a microbios, animales o plantas que afecten mi salud|Riesgos higiénicos|PROT-14|Gestión de condiciones físicas y ergonomía|Condiciones físicas saludables|Condiciones de trabajo saludable##
## |7_intra|A|Intralaboral|Demandas ambientales y de esfuerzo físico|En mi trabajo me preocupa estar expuesto a sustancias químicas que afecten mi salud|Riesgos higiénicos|PROT-14|Gestión de condiciones físicas y ergonomía|Condiciones físicas saludables|Condiciones de trabajo saludable##
## |8_intra|A|Intralaboral|Demandas ambientales y de esfuerzo físico|Mi trabajo me exige hacer mucho esfuerzo físico|Riesgos biomecánicos|PROT-14|Gestión de condiciones físicas y ergonomía|Condiciones físicas saludables|Condiciones de trabajo saludable##
## |9_intra|A|Intralaboral|Demandas ambientales y de esfuerzo físico|Los equipos o herramientas con los que trabajo son muy cómodos|Riesgos biomecánicos|PROT-14|Gestión de condiciones físicas y ergonomía|Condiciones físicas saludables|Condiciones de trabajo saludable##
## |11_intra|A|Intralaboral|Demandas ambientales y de esfuerzo físico|Me preocupa accidentarme en mi trabajo|Seguridad percibida|PROT-14|Gestión de condiciones físicas y ergonomía|Condiciones físicas saludables|Condiciones de trabajo saludable##
## |1_intra|B|Intralaboral|Demandas ambientales y de esfuerzo físico|El ruido en el lugar donde trabajo es molesto|Confort del entorno|PROT-14|Gestión de condiciones físicas y ergonomía|Condiciones físicas saludables|Condiciones de trabajo saludable##
## |2_intra|B|Intralaboral|Demandas ambientales y de esfuerzo físico|En el lugar donde trabajo hace mucho frio|Confort del entorno|PROT-14|Gestión de condiciones físicas y ergonomía|Condiciones físicas saludables|Condiciones de trabajo saludable##
## |3_intra|B|Intralaboral|Demandas ambientales y de esfuerzo físico|En el lugar donde trabajo hace mucho calor|Confort del entorno|PROT-14|Gestión de condiciones físicas y ergonomía|Condiciones físicas saludables|Condiciones de trabajo saludable##
## |4_intra|B|Intralaboral|Demandas ambientales y de esfuerzo físico|El aire en el lugar donde trabajo es fresco y agradable|Confort del entorno|PROT-14|Gestión de condiciones físicas y ergonomía|Condiciones físicas saludables|Condiciones de trabajo saludable##
## |5_intra|B|Intralaboral|Demandas ambientales y de esfuerzo físico|La luz del sitio donde trabajo es agradable|Confort del entorno|PROT-14|Gestión de condiciones físicas y ergonomía|Condiciones físicas saludables|Condiciones de trabajo saludable##
## |6_intra|B|Intralaboral|Demandas ambientales y de esfuerzo físico|El espacio donde trabajo es cómodo|Confort del entorno|PROT-14|Gestión de condiciones físicas y ergonomía|Condiciones físicas saludables|Condiciones de trabajo saludable##
## |12_intra|B|Intralaboral|Demandas ambientales y de esfuerzo físico|El lugar donde trabajo es limpio y ordenado|Orden percibido|PROT-14|Gestión de condiciones físicas y ergonomía|Condiciones físicas saludables|Condiciones de trabajo saludable##
## |8_intra|B|Intralaboral|Demandas ambientales y de esfuerzo físico|Mi trabajo me exige hacer mucho esfuerzo físico|Riesgos biomecánicos|PROT-14|Gestión de condiciones físicas y ergonomía|Condiciones físicas saludables|Condiciones de trabajo saludable##
## |9_intra|B|Intralaboral|Demandas ambientales y de esfuerzo físico|Los equipos o herramientas con los que trabajo son muy cómodos|Riesgos biomecánicos|PROT-14|Gestión de condiciones físicas y ergonomía|Condiciones físicas saludables|Condiciones de trabajo saludable##
## |10_intra|B|Intralaboral|Demandas ambientales y de esfuerzo físico|En mi trabajo me preocupa estar expuesto a microbios, animales o plantas que afecten mi salud|Riesgos higiénicos|PROT-14|Gestión de condiciones físicas y ergonomía|Condiciones físicas saludables|Condiciones de trabajo saludable##
## |7_intra|B|Intralaboral|Demandas ambientales y de esfuerzo físico|En mi trabajo me preocupa estar expuesto a sustancias químicas que afecten mi salud|Riesgos higiénicos|PROT-14|Gestión de condiciones físicas y ergonomía|Condiciones físicas saludables|Condiciones de trabajo saludable##
## |11_intra|B|Intralaboral|Demandas ambientales y de esfuerzo físico|Me preocupa accidentarme en mi trabajo|Seguridad percibida|PROT-14|Gestión de condiciones físicas y ergonomía|Condiciones físicas saludables|Condiciones de trabajo saludable##
## |16_intra|A|Intralaboral|Demandas de carga mental|Mi trabajo me exige hacer mucho esfuerzo mental|Atención y concentración sostenida|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |17_intra|A|Intralaboral|Demandas de carga mental|Mi trabajo me exige estar muy concentrado|Atención y concentración sostenida|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |21_intra|A|Intralaboral|Demandas de carga mental|Mi trabajo requiere que me fije en pequeños detalles|Atención y concentración sostenida|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |44_intra|A|Intralaboral|Control y autonomía sobre el trabajo|Puedo decidir cuánto trabajo hago en el día|Autogestión del volumen y ritmo|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |45_intra|A|Intralaboral|Control y autonomía sobre el trabajo|Puedo decidir la velocidad a la que trabajo|Autogestión del volumen y ritmo|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |46_intra|A|Intralaboral|Control y autonomía sobre el trabajo|Puedo cambiar el orden de las actividades en mi trabajo|Autogestión del volumen y ritmo|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |18_intra|A|Intralaboral|Demandas de carga mental|Mi trabajo me exige memorizar mucha información|Carga de memoria|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |32_intra|A|Intralaboral|Demandas cuantitativas|En mi trabajo es posible tomar pausas para descansar|Ritmo de trabajo|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |43_intra|A|Intralaboral|Demandas cuantitativas|Puedo tomar pausas cuando las necesito|Ritmo de trabajo|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |47_intra|A|Intralaboral|Demandas cuantitativas|Puedo parar un momento mi trabajo para atender algún asunto personal|Ritmo de trabajo|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |20_intra|A|Intralaboral|Demandas de carga mental|Mi trabajo me exige atender muchos asuntos al mismo tiempo|Simultaneidad|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |13_intra|A|Intralaboral|Demandas cuantitativas|Por la cantidad de trabajo que tengo debo quedarme tiempo adicional|Volumen de trabajo|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |14_intra|A|Intralaboral|Demandas cuantitativas|Me alcanza el tiempo de trabajo para tener al día mis deberes|Volumen de trabajo|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |15_intra|A|Intralaboral|Demandas cuantitativas|Por la cantidad de trabajo que tengo debo trabajar sin parar|Volumen de trabajo|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |16_intra|B|Intralaboral|Demandas de carga mental|Mi trabajo me exige hacer mucho esfuerzo mental|Atención y concentración sostenida|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |17_intra|B|Intralaboral|Demandas de carga mental|Mi trabajo me exige estar muy concentrado|Atención y concentración sostenida|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |20_intra|B|Intralaboral|Demandas de carga mental|Mi trabajo requiere que me fije en pequeños detalles|Atención y concentración sostenida|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |34_intra|B|Intralaboral|Control y autonomía sobre el trabajo|Puedo decidir cuánto trabajo hago en el día|Autogestión del volumen y ritmo|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |35_intra|B|Intralaboral|Control y autonomía sobre el trabajo|Puedo decidir la velocidad a la que trabajo|Autogestión del volumen y ritmo|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |36_intra|B|Intralaboral|Control y autonomía sobre el trabajo|Puedo cambiar el orden de las actividades en mi trabajo|Autogestión del volumen y ritmo|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |18_intra|B|Intralaboral|Demandas de carga mental|Mi trabajo me exige memorizar mucha información|Carga de memoria|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |19_intra|B|Intralaboral|Demandas de carga mental|En mi trabajo tengo que hacer cálculos matemáticos|Carga de memoria|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |22_intra|B|Intralaboral|Demandas cuantitativas|En mi trabajo es posible tomar pausas para descansar|Ritmo de trabajo|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |33_intra|B|Intralaboral|Demandas cuantitativas|Puedo tomar pausas cuando las necesito|Ritmo de trabajo|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |37_intra|B|Intralaboral|Demandas cuantitativas|Puedo para un momento mi trabajo para atender un asunto personal|Ritmo de trabajo|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |13_intra|B|Intralaboral|Demandas cuantitativas|Por la cantidad de trabajo que tengo debo quedarme tiempo adicional|Volumen de trabajo|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |14_intra|B|Intralaboral|Demandas cuantitativas|Me alcanza el tiempo de trabajo para tener al día mis deberes|Volumen de trabajo|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |15_intra|B|Intralaboral|Demandas cuantitativas|Por la cantidad de trabajo que tengo debo trabajar sin parar|Volumen de trabajo|PROT-15|Gestión de la carga de trabajo|Carga de trabajo saludable|Condiciones de trabajo saludable##
## |33_intra|A|Intralaboral|Demandas de la jornada de trabajo|Mi trabajo me exige laborar en días de descanso, festivos o fines de semana|Descansos programados|PROT-16|Gestión de jornadas de trabajo|Jornadas de trabajo saludables|Condiciones de trabajo saludable##
## |34_intra|A|Intralaboral|Demandas de la jornada de trabajo|En mi trabajo puedo tomar fines de semana o días de descanso al mes|Descansos programados|PROT-16|Gestión de jornadas de trabajo|Jornadas de trabajo saludables|Condiciones de trabajo saludable##
## |31_intra|A|Intralaboral|Demandas de la jornada de trabajo|Trabajo en horario de noche|Trabajo nocturno|PROT-16|Gestión de jornadas de trabajo|Jornadas de trabajo saludables|Condiciones de trabajo saludable##
## |23_intra|B|Intralaboral|Demandas de la jornada de trabajo|Mi trabajo me exige laborar en días de descanso, festivos o fines de semana|Descansos programados|PROT-16|Gestión de jornadas de trabajo|Jornadas de trabajo saludables|Condiciones de trabajo saludable##
## |24_intra|B|Intralaboral|Demandas de la jornada de trabajo|En mi trabajo puedo tomar fines de semana o días de descanso al mes|Descansos programados|PROT-16|Gestión de jornadas de trabajo|Jornadas de trabajo saludables|Condiciones de trabajo saludable##
## |21_intra|B|Intralaboral|Demandas de la jornada de trabajo|Trabajo en horario de noche|Trabajo nocturno|PROT-16|Gestión de jornadas de trabajo|Jornadas de trabajo saludables|Condiciones de trabajo saludable##
## |52_intra|A|Intralaboral|Consistencia del rol|Los cambios que se presentan en mi trabajo dificultan mi labor|Claridad e impacto del cambio|PROT-17|Gestión del cambio organizacional|Cambio organizacional|Condiciones de trabajo saludable##
## |48_intra|A|Intralaboral|Participación y manejo del cambio|Los cambios en mi trabajo han sido beneficiosos|Claridad e impacto del cambio|PROT-17|Gestión del cambio organizacional|Cambio organizacional|Condiciones de trabajo saludable##
## |49_intra|A|Intralaboral|Participación y manejo del cambio|Me explican claramente los cambios que ocurren en mi trabajo|Claridad e impacto del cambio|PROT-17|Gestión del cambio organizacional|Cambio organizacional|Condiciones de trabajo saludable##
## |50_intra|A|Intralaboral|Participación y manejo del cambio|Puedo dar sugerencias sobre los cambios que ocurren en mi trabajo|Participación en los cambios|PROT-17|Gestión del cambio organizacional|Cambio organizacional|Condiciones de trabajo saludable##
## |51_intra|A|Intralaboral|Participación y manejo del cambio|Cuando se presentan cambios en mi trabajo se tiene en cuenta mis ideas y sugerencias|Participación en los cambios|PROT-17|Gestión del cambio organizacional|Cambio organizacional|Condiciones de trabajo saludable##
## |38_intra|B|Intralaboral|Participación y manejo del cambio|Me explican claramente los cambios que ocurren en mi trabajo|Claridad e impacto del cambio|PROT-17|Gestión del cambio organizacional|Cambio organizacional|Condiciones de trabajo saludable##
## |39_intra|B|Intralaboral|Participación y manejo del cambio|Puedo dar sugerencias sobre los cambios que ocurren en mi trabajo|Participación en los cambios|PROT-17|Gestión del cambio organizacional|Cambio organizacional|Condiciones de trabajo saludable##
## |40_intra|B|Intralaboral|Participación y manejo del cambio|Cuando se presentan cambios en mi trabajo se tiene en cuenta mis ideas y sugerencias|Participación en los cambios|PROT-17|Gestión del cambio organizacional|Cambio organizacional|Condiciones de trabajo saludable##
## |96_intra|A|Intralaboral|Reconocimiento y compensación|En la empresa me pagan a tiempo mi salario|Cumplimiento en retribución|PROT-20|Gestión del Engagement organizacional|Engagement organizacional|Condiciones de trabajo saludable##
## |97_intra|A|Intralaboral|Reconocimiento y compensación|El pago que recibo es el que me ofreció la empresa|Cumplimiento en retribución|PROT-20|Gestión del Engagement organizacional|Engagement organizacional|Condiciones de trabajo saludable##
## |100_intra|A|Intralaboral|Reconocimiento y compensación|Las personas que hacen bien el trabajo pueden progresar en la empresa|Desarrollo y movilidad interna|PROT-20|Gestión del Engagement organizacional|Engagement organizacional|Condiciones de trabajo saludable##
## |99_intra|A|Intralaboral|Reconocimiento y compensación|En mi trabajo tengo posibilidades de progresar|Desarrollo y movilidad interna|PROT-20|Gestión del Engagement organizacional|Engagement organizacional|Condiciones de trabajo saludable##
## |102_intra|A|Intralaboral|Recompensas derivadas de la pertenencia a la|Mi trabajo en la empresa es estable|Estabilidad y confianza|PROT-20|Gestión del Engagement organizacional|Engagement organizacional|Condiciones de trabajo saludable##
## |95_intra|A|Intralaboral|Recompensas derivadas de la pertenencia a la|En la empresa confían en mi trabajo|Estabilidad y confianza|PROT-20|Gestión del Engagement organizacional|Engagement organizacional|Condiciones de trabajo saludable##
## |98_intra|A|Intralaboral|Reconocimiento y compensación|El pago que recibo es el que merezco por el trabajo que realizo|Expectativa en la retribución|PROT-20|Gestión del Engagement organizacional|Engagement organizacional|Condiciones de trabajo saludable##
## |103_intra|A|Intralaboral|Recompensas derivadas de la pertenencia a la|El trabajo que hago me hace sentir bien|Orgullo de pertenencia|PROT-20|Gestión del Engagement organizacional|Engagement organizacional|Condiciones de trabajo saludable##
## |104_intra|A|Intralaboral|Recompensas derivadas de la pertenencia a la|Siento orgullo de trabajar en esta empresa|Orgullo de pertenencia|PROT-20|Gestión del Engagement organizacional|Engagement organizacional|Condiciones de trabajo saludable##
## |105_intra|A|Intralaboral|Recompensas derivadas de la pertenencia a la|Hablo bien de la empresa con otras personas|Orgullo de pertenencia|PROT-20|Gestión del Engagement organizacional|Engagement organizacional|Condiciones de trabajo saludable##
## |101_intra|A|Intralaboral|Reconocimiento y compensación|La empresa se preocupa por el bienestar de los trabajadores|Valoración del bienestar|PROT-20|Gestión del Engagement organizacional|Engagement organizacional|Condiciones de trabajo saludable##
## |79_intra|B|Intralaboral|Reconocimiento y compensación|En la empresa me pagan a tiempo mi salario|Cumplimiento en retribución|PROT-20|Gestión del Engagement organizacional|Engagement organizacional|Condiciones de trabajo saludable##
## |80_intra|B|Intralaboral|Reconocimiento y compensación|El pago que recibo es el que me ofreció la empresa|Cumplimiento en retribución|PROT-20|Gestión del Engagement organizacional|Engagement organizacional|Condiciones de trabajo saludable##
## |82_intra|B|Intralaboral|Reconocimiento y compensación|En mi trabajo tengo posibilidades de progresar|Desarrollo y movilidad interna|PROT-20|Gestión del Engagement organizacional|Engagement organizacional|Condiciones de trabajo saludable##
## |83_intra|B|Intralaboral|Reconocimiento y compensación|Las personas que hacen bien el trabajo pueden progresar en la empresa|Desarrollo y movilidad interna|PROT-20|Gestión del Engagement organizacional|Engagement organizacional|Condiciones de trabajo saludable##
## |85_intra|B|Intralaboral|Recompensas derivadas de la pertenencia a la|Mi trabajo en la empresa es estable|Estabilidad y confianza|PROT-20|Gestión del Engagement organizacional|Engagement organizacional|Condiciones de trabajo saludable##
## |81_intra|B|Intralaboral|Reconocimiento y compensación|El pago que recibo es el que merezco por el trabajo que realizo|Expectativa en la retribución|PROT-20|Gestión del Engagement organizacional|Engagement organizacional|Condiciones de trabajo saludable##
## |86_intra|B|Intralaboral|Recompensas derivadas de la pertenencia a la|El trabajo que hago me hace sentir bien|Orgullo de pertenencia|PROT-20|Gestión del Engagement organizacional|Engagement organizacional|Condiciones de trabajo saludable##
## |87_intra|B|Intralaboral|Recompensas derivadas de la pertenencia a la|Siento orgullo de trabajar en esta empresa|Orgullo de pertenencia|PROT-20|Gestión del Engagement organizacional|Engagement organizacional|Condiciones de trabajo saludable##
## |88_intra|B|Intralaboral|Recompensas derivadas de la pertenencia a la|Hablo bien de la empresa con otras personas|Orgullo de pertenencia|PROT-20|Gestión del Engagement organizacional|Engagement organizacional|Condiciones de trabajo saludable##
## |84_intra|B|Intralaboral|Reconocimiento y compensación|La empresa se preocupa por el bienestar de los trabajadores|Valoración del bienestar|PROT-20|Gestión del Engagement organizacional|Engagement organizacional|Condiciones de trabajo saludable##
## |78_intra|A|Intralaboral|Relaciones sociales en el trabajo|Siento que puedo confiar en mis compañeros de trabajo|Apoyo emocional|PROT-18|Convivencia, relacionamiento y prevención de violencia|Convivencia y relacionamiento|Entorno y clima de trabajo saludable##
## |89_intra|A|Intralaboral|Relaciones sociales en el trabajo|Algunos compañeros de trabajo me escuchan cuando tengo problemas|Apoyo emocional|PROT-18|Convivencia, relacionamiento y prevención de violencia|Convivencia y relacionamiento|Entorno y clima de trabajo saludable##
## |85_intra|A|Intralaboral|Relaciones sociales en el trabajo|Cuando tenemos que realizar trabajo de grupo los compañeros colaboran|Apoyo social e instrumental|PROT-18|Convivencia, relacionamiento y prevención de violencia|Convivencia y relacionamiento|Entorno y clima de trabajo saludable##
## |86_intra|A|Intralaboral|Relaciones sociales en el trabajo|Es fácil poner de acuerdo el grupo para hacer el trabajo|Apoyo social e instrumental|PROT-18|Convivencia, relacionamiento y prevención de violencia|Convivencia y relacionamiento|Entorno y clima de trabajo saludable##
## |87_intra|A|Intralaboral|Relaciones sociales en el trabajo|Mis compañeros de trabajo me ayudan cuando tengo dificultades|Apoyo social e instrumental|PROT-18|Convivencia, relacionamiento y prevención de violencia|Convivencia y relacionamiento|Entorno y clima de trabajo saludable##
## |88_intra|A|Intralaboral|Relaciones sociales en el trabajo|En mi trabajo las personas nos apoyamos unos a otros|Apoyo social e instrumental|PROT-18|Convivencia, relacionamiento y prevención de violencia|Convivencia y relacionamiento|Entorno y clima de trabajo saludable##
## |76_intra|A|Intralaboral|Relaciones sociales en el trabajo|Me agrada el ambiente de mi grupo de trabajo|Cohesión y sentido de pertenencia|PROT-18|Convivencia, relacionamiento y prevención de violencia|Convivencia y relacionamiento|Entorno y clima de trabajo saludable##
## |79_intra|A|Intralaboral|Relaciones sociales en el trabajo|Me siento a gusto con mis compañeros de trabajo|Cohesión y sentido de pertenencia|PROT-18|Convivencia, relacionamiento y prevención de violencia|Convivencia y relacionamiento|Entorno y clima de trabajo saludable##
## |82_intra|A|Intralaboral|Relaciones sociales en el trabajo|Hay integración en mi grupo de trabajo|Cohesión y sentido de pertenencia|PROT-18|Convivencia, relacionamiento y prevención de violencia|Convivencia y relacionamiento|Entorno y clima de trabajo saludable##
## |83_intra|A|Intralaboral|Relaciones sociales en el trabajo|Mi grupo de trabajo es muy unido|Cohesión y sentido de pertenencia|PROT-18|Convivencia, relacionamiento y prevención de violencia|Convivencia y relacionamiento|Entorno y clima de trabajo saludable##
## |84_intra|A|Intralaboral|Relaciones sociales en el trabajo|Las personas en mi trabajo me hacen sentir parte del grupo|Cohesión y sentido de pertenencia|PROT-18|Convivencia, relacionamiento y prevención de violencia|Convivencia y relacionamiento|Entorno y clima de trabajo saludable##
## |77_intra|A|Intralaboral|Relaciones sociales en el trabajo|En mi grupo de trabajo me tratan de forma respetuosa|Convivencia y respeto|PROT-18|Convivencia, relacionamiento y prevención de violencia|Convivencia y relacionamiento|Entorno y clima de trabajo saludable##
## |80_intra|A|Intralaboral|Relaciones sociales en el trabajo|En mi grupo de trabajo algunas personas me maltratan|Convivencia y respeto|PROT-18|Convivencia, relacionamiento y prevención de violencia|Convivencia y relacionamiento|Entorno y clima de trabajo saludable##
## |81_intra|A|Intralaboral|Relaciones sociales en el trabajo|Entre compañeros solucionamos los problemas de forma respetuosa|Convivencia y respeto|PROT-18|Convivencia, relacionamiento y prevención de violencia|Convivencia y relacionamiento|Entorno y clima de trabajo saludable##
## |64_intra|B|Intralaboral|Relaciones sociales en el trabajo|Siento que puedo confiar en mis compañeros de trabajo|Apoyo emocional|PROT-18|Convivencia, relacionamiento y prevención de violencia|Convivencia y relacionamiento|Entorno y clima de trabajo saludable##
## |73_intra|B|Intralaboral|Relaciones sociales en el trabajo|Algunos compañeros de trabajo me escuchan cuando tengo problemas|Apoyo emocional|PROT-18|Convivencia, relacionamiento y prevención de violencia|Convivencia y relacionamiento|Entorno y clima de trabajo saludable##
## |69_intra|B|Intralaboral|Relaciones sociales en el trabajo|Cuando tenemos que realizar trabajo de grupo los compañeros colaboran|Apoyo social e instrumental|PROT-18|Convivencia, relacionamiento y prevención de violencia|Convivencia y relacionamiento|Entorno y clima de trabajo saludable##
## |70_intra|B|Intralaboral|Relaciones sociales en el trabajo|Es fácil poner de acuerdo el grupo para hacer el trabajo|Apoyo social e instrumental|PROT-18|Convivencia, relacionamiento y prevención de violencia|Convivencia y relacionamiento|Entorno y clima de trabajo saludable##
## |71_intra|B|Intralaboral|Relaciones sociales en el trabajo|Mis compañeros de trabajo me ayudan cuando tengo dificultades|Apoyo social e instrumental|PROT-18|Convivencia, relacionamiento y prevención de violencia|Convivencia y relacionamiento|Entorno y clima de trabajo saludable##
## |72_intra|B|Intralaboral|Relaciones sociales en el trabajo|En mi trabajo las personas nos apoyamos unos a otros|Apoyo social e instrumental|PROT-18|Convivencia, relacionamiento y prevención de violencia|Convivencia y relacionamiento|Entorno y clima de trabajo saludable##
## |62_intra|B|Intralaboral|Relaciones sociales en el trabajo|Me agrada el ambiente de mi grupo de trabajo|Cohesión y sentido de pertenencia|PROT-18|Convivencia, relacionamiento y prevención de violencia|Convivencia y relacionamiento|Entorno y clima de trabajo saludable##
## |65_intra|B|Intralaboral|Relaciones sociales en el trabajo|Me siento a gusto con mis compañeros de trabajo|Cohesión y sentido de pertenencia|PROT-18|Convivencia, relacionamiento y prevención de violencia|Convivencia y relacionamiento|Entorno y clima de trabajo saludable##
## |68_intra|B|Intralaboral|Relaciones sociales en el trabajo|Mi grupo de trabajo es muy unido|Cohesión y sentido de pertenencia|PROT-18|Convivencia, relacionamiento y prevención de violencia|Convivencia y relacionamiento|Entorno y clima de trabajo saludable##
## |63_intra|B|Intralaboral|Relaciones sociales en el trabajo|En mi grupo de trabajo me tratan de forma respetuosa|Convivencia y respeto|PROT-18|Convivencia, relacionamiento y prevención de violencia|Convivencia y relacionamiento|Entorno y clima de trabajo saludable##
## |66_intra|B|Intralaboral|Relaciones sociales en el trabajo|En mi grupo de trabajo algunas personas me maltratan|Convivencia y respeto|PROT-18|Convivencia, relacionamiento y prevención de violencia|Convivencia y relacionamiento|Entorno y clima de trabajo saludable##
## |67_intra|B|Intralaboral|Relaciones sociales en el trabajo|Entre compañeros solucionamos los problemas de forma respetuosa|Convivencia y respeto|PROT-18|Convivencia, relacionamiento y prevención de violencia|Convivencia y relacionamiento|Entorno y clima de trabajo saludable##
## |71_intra|A|Intralaboral|Características del liderazgo|Mi jefe me ayuda a sentirme bien en el trabajo|Apoyo social y seguridad psicológica|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |73_intra|A|Intralaboral|Características del liderazgo|Siento que puedo confiar en mi jefe|Apoyo social y seguridad psicológica|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |74_intra|A|Intralaboral|Características del liderazgo|Mi jefe me escucha cuando tengo problemas de trabajo|Apoyo social y seguridad psicológica|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |75_intra|A|Intralaboral|Características del liderazgo|Mi jefe me brinda su apoyo cuando lo necesito|Apoyo social y seguridad psicológica|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |92_intra|A|Intralaboral|Retroalimentación del desempeño|La información que recibo sobre mi rendimiento en el trabajo es clara|Calidad y utilidad de la retroalimentación|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |93_intra|A|Intralaboral|Retroalimentación del desempeño|La forma como evalúan mi trabajo en la empresa me ayuda a mejorar|Calidad y utilidad de la retroalimentación|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |63_intra|A|Intralaboral|Características del liderazgo|Mi jefe me da instrucciones claras|Claridad operativa y flujo de información|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |68_intra|A|Intralaboral|Características del liderazgo|Mi jefe me comunica a tiempo la información relacionada con el trabajo|Claridad operativa y flujo de información|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |65_intra|A|Intralaboral|Características del liderazgo|Mi jefe tiene en cuenta mis puntos de vista y opiniones|Desarrollo, participación e inspiración|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |66_intra|A|Intralaboral|Características del liderazgo|Mi jefe me anima para hacer mejor mi trabajo|Desarrollo, participación e inspiración|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |70_intra|A|Intralaboral|Características del liderazgo|Mi jefe me ayuda a progresar en el trabajo|Desarrollo, participación e inspiración|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |90_intra|A|Intralaboral|Retroalimentación del desempeño|Me informan sobre lo que hago bien en mi trabajo|Reconocimiento y refuerzo positivo|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |91_intra|A|Intralaboral|Retroalimentación del desempeño|Me informan sobre lo que debo mejorar en mi trabajo|Retroalimentación correctiva oportuna|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |94_intra|A|Intralaboral|Retroalimentación del desempeño|Me informan a tiempo sobre lo que debo mejorar en el trabajo|Retroalimentación correctiva oportuna|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |64_intra|A|Intralaboral|Características del liderazgo|Mi jefe me ayuda a organizar mejor el trabajo|Soporte instrumental y facilitación de la tarea|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |67_intra|A|Intralaboral|Características del liderazgo|Mi jefe distribuye las tareas de forma que me facilita el trabajo|Soporte instrumental y facilitación de la tarea|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |69_intra|A|Intralaboral|Características del liderazgo|La orientación que me da mi jefe me ayuda a hacer mejor el trabajo|Soporte instrumental y facilitación de la tarea|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |72_intra|A|Intralaboral|Características del liderazgo|Mi me jefe ayuda a solucionar los problemas que se presentan en el trabajo|Soporte instrumental y facilitación de la tarea|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |56_intra|B|Intralaboral|Características del liderazgo|Mi jefe me ayuda a sentirme bien en el trabajo|Apoyo social y seguridad psicológica|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |58_intra|B|Intralaboral|Características del liderazgo|Mi jefe me trata con respeto|Apoyo social y seguridad psicológica|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |59_intra|B|Intralaboral|Características del liderazgo|Siento que puedo confiar en mi jefe|Apoyo social y seguridad psicológica|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |60_intra|B|Intralaboral|Características del liderazgo|Mi jefe me escucha cuando tengo problemas de trabajo|Apoyo social y seguridad psicológica|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |61_intra|B|Intralaboral|Características del liderazgo|Mi jefe me brinda su apoyo cuando lo necesito|Apoyo social y seguridad psicológica|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |76_intra|B|Intralaboral|Retroalimentación del desempeño|La información que recibo sobre mi rendimiento en el trabajo es clara|Calidad y utilidad de la retroalimentación|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |77_intra|B|Intralaboral|Retroalimentación del desempeño|La forma como evalúan mi trabajo en la empresa me ayuda a mejorar|Calidad y utilidad de la retroalimentación|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |53_intra|B|Intralaboral|Características del liderazgo|Mi jefe me comunica a tiempo la información relacionada con el trabajo|Claridad operativa y flujo de información|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |50_intra|B|Intralaboral|Características del liderazgo|Mi jefe tiene en cuenta mis puntos de vista y opiniones|Desarrollo, participación e inspiración|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |51_intra|B|Intralaboral|Características del liderazgo|Mi jefe me anima para hacer mejor mi trabajo|Desarrollo, participación e inspiración|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |55_intra|B|Intralaboral|Características del liderazgo|Mi jefe me ayuda a progresar en el trabajo|Desarrollo, participación e inspiración|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |74_intra|B|Intralaboral|Retroalimentación del desempeño|Me informan sobre lo que hago bien en mi trabajo|Reconocimiento y refuerzo positivo|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |75_intra|B|Intralaboral|Retroalimentación del desempeño|Me informan sobre lo que debo mejorar en mi trabajo|Retroalimentación correctiva oportuna|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |78_intra|B|Intralaboral|Retroalimentación del desempeño|Me informan a tiempo sobre lo que debo mejorar en el trabajo|Retroalimentación correctiva oportuna|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |49_intra|B|Intralaboral|Características del liderazgo|Mi jefe me ayuda a organizar mejor el trabajo|Soporte instrumental y facilitación de la tarea|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |52_intra|B|Intralaboral|Características del liderazgo|Mi jefe distribuye las tareas de forma que me facilita el trabajo|Soporte instrumental y facilitación de la tarea|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |54_intra|B|Intralaboral|Características del liderazgo|La orientación que me da mi jefe me ayuda a hacer mejor el trabajo|Soporte instrumental y facilitación de la tarea|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |57_intra|B|Intralaboral|Características del liderazgo|Mi me jefe ayuda a solucionar los problemas que se presentan en el trabajo|Soporte instrumental y facilitación de la tarea|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |117_intra|A|Intralaboral|Relación con los colaboradores (subordinados)|Tengo colaboradores que comunican tarde los asuntos de trabajo|Estresores para los lideres en su gestión de equipos|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |120_intra|A|Intralaboral|Relación con los colaboradores (subordinados)|Tengo colaboradores que guardan silencio cuando les piden opiniones|Estresores para los lideres en su gestión de equipos|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |125_intra|A|Intralaboral|Relación con los colaboradores (subordinados)|Tengo colaboradores que ignoran las sugerencias para mejorar su trabajo|Estresores para los lideres en su gestión de equipos|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |118_intra|A|Intralaboral|Relación con los colaboradores (subordinados)|Tengo colaboradores que tienen comportamientos irrespetuosos|Estresores para los lideres en su gestión de equipos|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |122_intra|A|Intralaboral|Relación con los colaboradores (subordinados)|Tengo colaboradores que expresan de forma irrespetuosa sus desacuerdos|Estresores para los lideres en su gestión de equipos|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |119_intra|A|Intralaboral|Relación con los colaboradores (subordinados)|Tengo colaboradores que dificultan la organización del trabajo|Estresores para los lideres en su gestión de equipos|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |121_intra|A|Intralaboral|Relación con los colaboradores (subordinados)|Tengo colaboradores que dificultan el logro de los resultados del trabajo|Estresores para los lideres en su gestión de equipos|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |123_intra|A|Intralaboral|Relación con los colaboradores (subordinados)|Tengo colaboradores que cooperan poco cuando se necesita|Estresores para los lideres en su gestión de equipos|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |124_intra|A|Intralaboral|Relación con los colaboradores (subordinados)|Tengo colaboradores que me preocupan por su desempeño|Estresores para los lideres en su gestión de equipos|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##
## |116_intra|A|Intralaboral|Relación con los colaboradores (subordinados)|Soy jefe de otras personas en mi trabajo|Estresores para los lideres en su gestión de equipos|PROT-19|Ecosistema de liderazgo con impacto psicosocial|Ecosistema de liderazgo con impacto psicosocial|Entorno y clima de trabajo saludable##


## Paso 3. Instrucciones para calificación de indicadores, líneas de gestión y ejes## 
## 3.1 Calificar preguntas en indicadores con pesos relativos ##
## Antes de calificar el indicador, se invierte el sentido de la pregunta al indicador cuando aplique (escala 0 a 1) con formula (1 – valor). Se detalla cuando aplica esta inversión con true, false. Se debe aplicar la fórmula de invertir de manera discriminada a las preguntas que aplican en cada indicador tanto para A como para B, como las que aplican solo para A o solo para B A cada trabajador se le invierte su resultado segun corresponda##
## |forma_intra| id_pregunta| invertir_id_respuesta|##
## |A_y_B|1_capitalpsicologico|FALSE|
## |A_y_B|2_capitalpsicologico|FALSE|
## |A_y_B|3_capitalpsicologico|FALSE|
## |A_y_B|4_capitalpsicologico|FALSE|
## |A_y_B|5_capitalpsicologico|FALSE|
## |A_y_B|6_capitalpsicologico|FALSE|
## |A_y_B|7_capitalpsicologico|FALSE|
## |A_y_B|8_capitalpsicologico|FALSE|
## |A_y_B|9_capitalpsicologico|FALSE|
## |A_y_B|10_capitalpsicologico|FALSE|
## |A_y_B|11_capitalpsicologico|FALSE|
## |A_y_B|12_capitalpsicologico|FALSE|
## |A_y_B|1_afrontamiento|FALSE|
## |A_y_B|2_afrontamiento|FALSE|
## |A_y_B|3_afrontamiento|FALSE|
## |A_y_B|4_afrontamiento|FALSE|
## |A_y_B|5_afrontamiento|TRUE|
## |A_y_B|6_afrontamiento|TRUE|
## |A_y_B|7_afrontamiento|TRUE|
## |A_y_B|8_afrontamiento|TRUE|
## |A_y_B|9_afrontamiento|FALSE|
## |A_y_B|10_afrontamiento|FALSE|
## |A_y_B|11_afrontamiento|FALSE|
## |A_y_B|12_afrontamiento|FALSE|
## |A_y_B|10_estres|FALSE|
## |A_y_B|14_estres|FALSE|
## |A_y_B|21_estres|FALSE|
## |A_y_B|23_estres|FALSE|
## |A_y_B|24_estres|FALSE|
## |A_y_B|25_estres|FALSE|
## |A_y_B|29_estres|FALSE|
## |A_y_B|31_estres|FALSE|
## |A_y_B|12_estres|FALSE|
## |A_y_B|16_estres|FALSE|
## |A_y_B|27_estres|FALSE|
## |A_y_B|7_extra|TRUE|
## |A_y_B|8_extra|TRUE|
## |A_y_B|9_extra|TRUE|
## |A_y_B|18_extra|TRUE|
## |A_y_B|19_extra|TRUE|
## |A_y_B|20_extra|TRUE|
## |A_y_B|21_extra|TRUE|
## |A_y_B|23_extra|TRUE|
## |A_y_B|10_extra|TRUE|
## |A_y_B|11_extra|TRUE|
## |A_y_B|12_extra|TRUE|
## |A_y_B|13_extra|TRUE|
## |A_y_B|11_estres|FALSE|
## |A_y_B|9_estres|FALSE|
## |A_y_B|1_extra|TRUE|
## |A_y_B|2_extra|TRUE|
## |A_y_B|3_extra|TRUE|
## |A_y_B|4_extra|TRUE|
## |A_y_B|22_extra|TRUE|
## |A_y_B|25_extra|TRUE|
## |A_y_B|27_extra|TRUE|
## |A_y_B|5_extra|TRUE|
## |A_y_B|6_extra|TRUE|
## |A_y_B|29_extra|TRUE|
## |A_y_B|30_extra|TRUE|
## |A_y_B|31_extra|TRUE|
## |A_y_B|1_estres|FALSE|
## |A_y_B|2_estres|FALSE|
## |A_y_B|3_estres|FALSE|
## |A_y_B|4_estres|FALSE|
## |A_y_B|5_estres|FALSE|
## |A_y_B|6_estres|FALSE|
## |A_y_B|7_estres|FALSE|
## |A_y_B|8_estres|FALSE|
## |A_y_B|24_extra|FALSE|
## |A_y_B|26_extra|FALSE|
## |A_y_B|28_extra|FALSE|
## |A_y_B|14_extra|TRUE|
## |A_y_B|15_extra|TRUE|
## |A_y_B|16_extra|TRUE|
## |A_y_B|17_extra|TRUE|
## |B|25_intra|FALSE|
## |B|26_intra|FALSE|
## |A|35_intra|FALSE|
## |A|36_intra|FALSE|
## |B|27_intra|FALSE|
## |B|28_intra|FALSE|
## |A|37_intra|FALSE|
## |A|38_intra|FALSE|
## |B|29_intra|FALSE|
## |B|30_intra|FALSE|
## |A_y_B|13_estres|FALSE|
## |A_y_B|17_estres|FALSE|
## |A_y_B|18_estres|FALSE|
## |A_y_B|19_estres|FALSE|
## |A_y_B|20_estres|FALSE|
## |A_y_B|22_estres|FALSE|
## |A_y_B|15_estres|FALSE|
## |A_y_B|26_estres|FALSE|
## |A_y_B|28_estres|FALSE|
## |A_y_B|30_estres|FALSE|
## |B|42_intra|TRUE|
## |A|54_intra|TRUE|
## |B|41_intra|TRUE|
## |B|43_intra|TRUE|
## |B|44_intra|TRUE|
## |A|53_intra|TRUE|
## |A|55_intra|TRUE|
## |A|57_intra|TRUE|
## |A|19_intra|FALSE|
## |A|25_intra|FALSE|
## |A|29_intra|TRUE|
## |A|28_intra|TRUE|
## |A|30_intra|TRUE|
## |B|45_intra|TRUE|
## |A|56_intra|TRUE|
## |A|58_intra|TRUE|
## |A|59_intra|TRUE|
## |A|22_intra|FALSE|
## |A|23_intra|FALSE|
## |A|24_intra|FALSE|
## |A|26_intra|FALSE|
## |A|27_intra|FALSE|
## |B|46_intra|TRUE|
## |A|60_intra|TRUE|
## |B|31_intra|TRUE|
## |A|40_intra|TRUE|
## |A|42_intra|TRUE|
## |B|29_intra|TRUE|
## |B|30_intra|TRUE|
## |B|32_intra|TRUE|
## |A|39_intra|TRUE|
## |A|41_intra|TRUE|
## |A|43_intra|TRUE|
## |A|44_intra|TRUE|
## |B|47_intra|TRUE|
## |B|48_intra|TRUE|
## |A|61_intra|TRUE|
## |A|62_intra|TRUE|
## |A|106_intra|FALSE|
## |A|113_intra|FALSE|
## |B|89_intra|FALSE|
## |B|98_intra|FALSE|
## |A|108_intra|FALSE|
## |A|109_intra|FALSE|
## |A|110_intra|FALSE|
## |A|111_intra|FALSE|
## |A|115_intra|FALSE|
## |B|91_intra|FALSE|
## |B|92_intra|FALSE|
## |B|93_intra|FALSE|
## |B|94_intra|FALSE|
## |B|97_intra|FALSE|
## |A|107_intra|FALSE|
## |A|112_intra|FALSE|
## |A|114_intra|FALSE|
## |B|90_intra|FALSE|
## |B|95_intra|FALSE|
## |B|96_intra|FALSE|
## |A|1_intra|TRUE|
## |A|2_intra|TRUE|
## |A|3_intra|TRUE|
## |A|4_intra|TRUE|
## |A|5_intra|TRUE|
## |A|6_intra|TRUE|
## |B|1_intra|TRUE|
## |B|2_intra|TRUE|
## |B|3_intra|TRUE|
## |B|4_intra|TRUE|
## |B|5_intra|TRUE|
## |B|6_intra|TRUE|
## |A|12_intra|TRUE|
## |B|12_intra|TRUE|
## |B|8_intra|FALSE|
## |B|9_intra|FALSE|
## |A|8_intra|FALSE|
## |A|9_intra|FALSE|
## |A|10_intra|FALSE|
## |A|7_intra|FALSE|
## |B|10_intra|FALSE|
## |B|7_intra|FALSE|
## |A|11_intra|TRUE|
## |B|11_intra|TRUE|
## |A|16_intra|FALSE|
## |A|17_intra|FALSE|
## |A|21_intra|FALSE|
## |B|16_intra|FALSE|
## |B|17_intra|FALSE|
## |B|20_intra|FALSE|
## |B|34_intra|TRUE|
## |B|35_intra|TRUE|
## |B|36_intra|TRUE|
## |A|44_intra|TRUE|
## |A|45_intra|TRUE|
## |A|46_intra|TRUE|
## |A|18_intra|FALSE|
## |B|18_intra|FALSE|
## |B|19_intra|FALSE|
## |B|22_intra|FALSE|
## |B|33_intra|FALSE|
## |B|37_intra|FALSE|
## |A|32_intra|FALSE|
## |A|43_intra|FALSE|
## |A|47_intra|FALSE|
## |A|20_intra|FALSE|
## |A|13_intra|FALSE|
## |A|14_intra|FALSE|
## |A|15_intra|FALSE|
## |B|13_intra|FALSE|
## |B|14_intra|FALSE|
## |B|15_intra|FALSE|
## |B|23_intra|TRUE|
## |B|24_intra|TRUE|
## |A|33_intra|TRUE|
## |A|34_intra|TRUE|
## |B|21_intra|FALSE|
## |A|31_intra|FALSE|
## |B|38_intra|TRUE|
## |A|48_intra|TRUE|
## |A|49_intra|TRUE|
## |A|52_intra|TRUE|
## |B|39_intra|TRUE|
## |B|40_intra|TRUE|
## |A|50_intra|TRUE|
## |A|51_intra|TRUE|
## |B|79_intra|TRUE|
## |B|80_intra|TRUE|
## |A|96_intra|TRUE|
## |A|97_intra|TRUE|
## |A|100_intra|TRUE|
## |A|99_intra|TRUE|
## |B|82_intra|TRUE|
## |B|83_intra|TRUE|
## |A|102_intra|TRUE|
## |A|95_intra|TRUE|
## |B|85_intra|TRUE|
## |B|81_intra|TRUE|
## |A|98_intra|TRUE|
## |A|103_intra|TRUE|
## |A|104_intra|TRUE|
## |A|105_intra|TRUE|
## |B|86_intra|TRUE|
## |B|87_intra|TRUE|
## |B|88_intra|TRUE|
## |A|101_intra|TRUE|
## |B|84_intra|TRUE|
## |B|64_intra|TRUE|
## |B|73_intra|TRUE|
## |A|78_intra|TRUE|
## |A|89_intra|TRUE|
## |B|69_intra|TRUE|
## |B|70_intra|TRUE|
## |B|71_intra|TRUE|
## |B|72_intra|TRUE|
## |A|85_intra|TRUE|
## |A|86_intra|TRUE|
## |A|87_intra|TRUE|
## |A|88_intra|TRUE|
## |B|62_intra|TRUE|
## |B|65_intra|TRUE|
## |B|68_intra|TRUE|
## |A|76_intra|TRUE|
## |A|79_intra|TRUE|
## |A|82_intra|TRUE|
## |A|83_intra|TRUE|
## |A|84_intra|TRUE|
## |B|63_intra|TRUE|
## |B|66_intra|TRUE|
## |B|67_intra|TRUE|
## |A|77_intra|TRUE|
## |A|80_intra|TRUE|
## |A|81_intra|TRUE|
## |B|56_intra|TRUE|
## |B|58_intra|TRUE|
## |B|59_intra|TRUE|
## |B|60_intra|TRUE|
## |B|61_intra|TRUE|
## |A|71_intra|TRUE|
## |A|73_intra|TRUE|
## |A|74_intra|TRUE|
## |A|75_intra|TRUE|
## |B|76_intra|TRUE|
## |B|77_intra|TRUE|
## |A|92_intra|TRUE|
## |A|93_intra|TRUE|
## |B|53_intra|TRUE|
## |A|63_intra|TRUE|
## |A|68_intra|TRUE|
## |A|116_intra|TRUE|
## |A|117_intra|TRUE|
## |A|120_intra|TRUE|
## |A|125_intra|TRUE|
## |A|118_intra|TRUE|
## |A|122_intra|TRUE|
## |A|119_intra|TRUE|
## |A|121_intra|TRUE|
## |A|123_intra|TRUE|
## |A|124_intra|TRUE|
## |B|50_intra|TRUE|
## |B|51_intra|TRUE|
## |B|52_intra|TRUE|
## |B|54_intra|TRUE|
## |B|55_intra|TRUE|
## |A|65_intra|TRUE|
## |A|66_intra|TRUE|
## |A|70_intra|TRUE|
## |B|74_intra|TRUE|
## |A|90_intra|TRUE|
## |B|75_intra|TRUE|
## |B|78_intra|TRUE|
## |A|91_intra|TRUE|
## |A|94_intra|TRUE|
## |B|49_intra|TRUE|
## |B|52_intra|TRUE|
## |B|54_intra|TRUE|
## |B|57_intra|TRUE|
## |A|64_intra|TRUE|
## |A|67_intra|TRUE|
## |A|69_intra|TRUE|
## |A|72_intra|TRUE|##

## Cuando se ha aplicado formula de inversión cuando aplique, según la agrupación de preguntas en indicadores, se califica cada indicador, aplicando promedio ponderado de las respuestas con peso relativo al indicador. En tabla se detalla el peso a aplicar en la ponderación . Se debe aplicar la formula de manera discriminada a las preguntas que aplican en cada indicador tanto para A como para B, como las que aplican solo para A o solo para B. Cada trabajador obtiene un resultado en cada indicador acorde a esta calificacion ponderada##

## |forma_intra| id_pregunta|peso|indicador ##
## A_y_B|1_capitalpsicologico|1|Firmeza y perseverancia	##
## A_y_B|2_capitalpsicologico|1|Optimismo del porvenir	##
## A_y_B|3_capitalpsicologico|1|Resolutividad	##  
## A_y_B|4_capitalpsicologico|1|Esperanza del futuro	##
## A_y_B|5_capitalpsicologico|1|Convicción por las metas	##
## A_y_B|6_capitalpsicologico|1|Esfuerzo y dedicación	##
## A_y_B|7_capitalpsicologico|1|Aprendizaje resiliente	##
## A_y_B|8_capitalpsicologico|1|Superación de adversidades	##
## A_y_B|9_capitalpsicologico|1|Actitud resiliente	##
## A_y_B|10_capitalpsicologico|1|Autoeficacia percibida	##
## A_y_B|11_capitalpsicologico|1|Sensación de logro	##
## A_y_B|12_capitalpsicologico|1|Locus de control interno	##
## A_y_B|1_afrontamiento|1|Dedicación	##
## A_y_B|2_afrontamiento|1|Orientación a la acción	##
## A_y_B|3_afrontamiento|1|Propositividad	##
## A_y_B|4_afrontamiento|1|Planificación	##
## A_y_B|5_afrontamiento|1|Autonegación	##  
## A_y_B|6_afrontamiento|1|Renuncia	##
## A_y_B|7_afrontamiento|1|Evitación cognitiva	##
## A_y_B|8_afrontamiento|1|Evitación conductual	##
## A_y_B|9_afrontamiento|1|Busqueda de Apoyo profesional	##
## A_y_B|10_afrontamiento|1|Busqueda de consejo y orientación	##
## A_y_B|11_afrontamiento|1|Busqueda de soporte emocional	##
## A_y_B|12_afrontamiento|1|Activa red de apoyo	##
## A_y_B|10_estres|0.35|Alteraciones cognitivas ##
## A_y_B|14_estres|0.45|Alteraciones cognitivas ##
## A_y_B|21_estres|0.2|Alteraciones cognitivas ##
## A_y_B|23_estres|0.1|Desgaste emocional ##
## A_y_B|24_estres|0.1|Desgaste emocional ##
## A_y_B|25_estres|0.1|Desgaste emocional ##
## A_y_B|29_estres|0.35|Desgaste emocional ##
## A_y_B|31_estres|0.35|Desgaste emocional ##
## A_y_B|12_estres|0.2|Pérdida de sentido ##
## A_y_B|16_estres|0.3|Pérdida de sentido ##
## A_y_B|27_estres|0.5|Pérdida de sentido ##
## A_y_B|7_extra|0.3|Accesibilidad del entorno ##
## A_y_B|8_extra|0.35|Accesibilidad del entorno ##
## A_y_B|9_extra|0.35|Accesibilidad del entorno ##
## A_y_B|18_extra|0.1|Apoyo social ##
## A_y_B|19_extra|0.1|Apoyo social ##
## A_y_B|20_extra|0.1|Apoyo social ##
## A_y_B|21_extra|0.35|Apoyo social ##
## A_y_B|23_extra|0.35|Apoyo social ##
## A_y_B|10_extra|0.25|Condiciones de la vivienda ##
## A_y_B|11_extra|0.25|Condiciones de la vivienda ##
## A_y_B|12_extra|0.25|Condiciones de la vivienda ##
## A_y_B|13_extra|0.25|Condiciones de la vivienda ##
## A_y_B|11_estres|0.5|Dererioro de relaciones sociales y aislamiento ##
## A_y_B|9_estres|0.5|Dererioro de relaciones sociales y aislamiento ##
## A_y_B|1_extra|0.25|Movilidad eficiente y desplazamiento saludable ##
## A_y_B|2_extra|0.25|Movilidad eficiente y desplazamiento saludable ##
## A_y_B|3_extra|0.25|Movilidad eficiente y desplazamiento saludable ##
## A_y_B|4_extra|0.25|Movilidad eficiente y desplazamiento saludable ##
## A_y_B|22_extra|0.4|Relación con el núcleo familiar ##
## A_y_B|25_extra|0.2|Relación con el núcleo familiar ##
## A_y_B|27_extra|0.4|Relación con el núcleo familiar ##
## A_y_B|5_extra|0.5|Seguridad del entorno ##
## A_y_B|6_extra|0.5|Seguridad del entorno ##
## A_y_B|29_extra|0.3|Bienestar financiero ##
## A_y_B|30_extra|0.35|Bienestar financiero ##
## A_y_B|31_extra|0.35|Bienestar financiero ##
## A_y_B|1_estres|0.2|Somatización y fatiga física ##
## A_y_B|2_estres|0.05|Somatización y fatiga física ##
## A_y_B|3_estres|0.05|Somatización y fatiga física ##
## A_y_B|4_estres|0.2|Somatización y fatiga física ##
## A_y_B|5_estres|0.4|Somatización y fatiga física ##
## A_y_B|6_estres|0.05|Somatización y fatiga física ##
## A_y_B|7_estres|0.03|Somatización y fatiga física ##
## A_y_B|8_estres|0.02|Somatización y fatiga física ##
## A_y_B|24_extra|0.25|Conflicto vida - trabajo ##
## A_y_B|26_extra|0.25|Conflicto vida - trabajo ##
## A_y_B|28_extra|0.5|Conflicto vida - trabajo ##
## A_y_B|14_extra|0.2|Equilibrio y recuperación ##
## A_y_B|15_extra|0.5|Equilibrio y recuperación ##
## A_y_B|16_extra|0.2|Equilibrio y recuperación ##
## A_y_B|17_extra|0.1|Equilibrio y recuperación ##
## B|25_intra|0.5|Impacto cognitivo-relacional ##
## B|26_intra|0.5|Impacto cognitivo-relacional ##
## A|35_intra|0.5|Impacto cognitivo-relacional ##
## A|36_intra|0.5|Impacto cognitivo-relacional ##
## B|27_intra|0.8|Interferencia temporal ##
## B|28_intra|0.2|Interferencia temporal ##
## A|37_intra|0.8|Interferencia temporal ##
## A|38_intra|0.2|Interferencia temporal ##
## A_y_B|13_estres|0.05|Desmotivación y desgaste laboral ##
## A_y_B|17_estres|0.1|Desmotivación y desgaste laboral ##
## A_y_B|18_estres|0.05|Desmotivación y desgaste laboral ##
## A_y_B|19_estres|0.2|Desmotivación y desgaste laboral ##
## A_y_B|20_estres|0.3|Desmotivación y desgaste laboral ##
## A_y_B|22_estres|0.3|Desmotivación y desgaste laboral ##
## A_y_B|15_estres|0.15|Conductas de riesgo ##
## A_y_B|26_estres|0.55|Conductas de riesgo ##
## A_y_B|28_estres|0.15|Conductas de riesgo ##
## A_y_B|30_estres|0.15|Conductas de riesgo ##
## B|42_intra|1|Autonomía decisional ##
## A|54_intra|1|Autonomía decisional ##
## B|41_intra|0.33|Claridad en funciones y objetivos del rol ##
## B|43_intra|0.33|Claridad en funciones y objetivos del rol ##
## B|44_intra|0.34|Claridad en funciones y objetivos del rol ##
## A|53_intra|0.33|Claridad en funciones y objetivos del rol ##
## A|55_intra|0.33|Claridad en funciones y objetivos del rol ##
## A|57_intra|0.34|Claridad en funciones y objetivos del rol ##
## A|19_intra|0.7|Decisiones críticas y resultados de gestión ##
## A|25_intra|0.3|Decisiones críticas y resultados de gestión ##
## A|29_intra|1|Integridad normativa ##
## A|28_intra|0.5|Pertinencia operativa ##
## A|30_intra|0.5|Pertinencia operativa ##
## B|45_intra|1|Propósito del rol y redes de soporte ##
## A|56_intra|0.7|Propósito del rol y redes de soporte ##
## A|58_intra|0.15|Propósito del rol y redes de soporte ##
## A|59_intra|0.15|Propósito del rol y redes de soporte ##
## A|22_intra|0.5|Responsabilidad por bienes y valores ##
## A|23_intra|0.5|Responsabilidad por bienes y valores ##
## A|24_intra|0.5|Responsabilidad por la integridad y salud de otros ##
## A|26_intra|0.5|Responsabilidad por la integridad y salud de otros ##
## A|27_intra|1|Contradicción en estructura de mando ##
## B|46_intra|1|Acceso a capacitación ##
## A|60_intra|1|Acceso a capacitación ##
## B|31_intra|1|Alineación de competencias y ajuste al cargo ##
## A|40_intra|0.2|Alineación de competencias y ajuste al cargo ##
## A|42_intra|0.8|Alineación de competencias y ajuste al cargo ##
## B|29_intra|0.25|Rol enriquecido ##
## B|30_intra|0.5|Rol enriquecido ##
## B|32_intra|0.25|Rol enriquecido ##
## A|39_intra|0.7|Rol enriquecido ##
## A|41_intra|0.3|Rol enriquecido ##
## B|47_intra|0.5|Pertinencia y aplicabilidad de la capacitación ##
## B|48_intra|0.5|Pertinencia y aplicabilidad de la capacitación ##
## A|61_intra|0.5|Pertinencia y aplicabilidad de la capacitación ##
## A|62_intra|0.5|Pertinencia y aplicabilidad de la capacitación ##
## A|106_intra|0.2|Disonancia emocional ##
## A|113_intra|0.8|Disonancia emocional ##
## B|89_intra|0.2|Disonancia emocional ##
## B|98_intra|0.8|Disonancia emocional ##
## A|108_intra|0.2|Exposición al sufrimiento ##
## A|109_intra|0.2|Exposición al sufrimiento ##
## A|110_intra|0.2|Exposición al sufrimiento ##
## A|111_intra|0.2|Exposición al sufrimiento ##
## A|115_intra|0.2|Exposición al sufrimiento ##
## B|91_intra|0.2|Exposición al sufrimiento ##
## B|92_intra|0.2|Exposición al sufrimiento ##
## B|93_intra|0.2|Exposición al sufrimiento ##
## B|94_intra|0.2|Exposición al sufrimiento ##
## B|97_intra|0.2|Exposición al sufrimiento ##
## A|107_intra|0.1|Hostilidad y violencia ##
## A|112_intra|0.7|Hostilidad y violencia ##
## A|114_intra|0.2|Hostilidad y violencia ##
## B|90_intra|0.1|Hostilidad y violencia ##
## B|95_intra|0.7|Hostilidad y violencia ##
## B|96_intra|0.2|Hostilidad y violencia ##
## A|1_intra|0.25|Confort del entorno ##
## A|2_intra|0.15|Confort del entorno ##
## A|3_intra|0.15|Confort del entorno ##
## A|4_intra|0.15|Confort del entorno ##
## A|5_intra|0.15|Confort del entorno ##
## A|6_intra|0.15|Confort del entorno ##
## B|1_intra|0.25|Confort del entorno ##
## B|2_intra|0.15|Confort del entorno ##
## B|3_intra|0.15|Confort del entorno ##
## B|4_intra|0.15|Confort del entorno ##
## B|5_intra|0.15|Confort del entorno ##
## B|6_intra|0.15|Confort del entorno ##
## A|12_intra|1|Orden percibido ##
## B|12_intra|1|Orden percibido ##
## B|8_intra|0.7|Riesgos biomecánicos ##
## B|9_intra|0.3|Riesgos biomecánicos ##
## A|8_intra|0.7|Riesgos biomecánicos ##
## A|9_intra|0.3|Riesgos biomecánicos ##
## A|10_intra|0.3|Riesgos higiénicos ##
## A|7_intra|0.7|Riesgos higiénicos ##
## B|10_intra|0.3|Riesgos higiénicos ##
## B|7_intra|0.7|Riesgos higiénicos ##
## A|11_intra|1|Seguridad percibida ##
## B|11_intra|1|Seguridad percibida ##
## A|16_intra|0.33|Atención y concentración sostenida ##
## A|17_intra|0.33|Atención y concentración sostenida ##
## A|21_intra|0.34|Atención y concentración sostenida ##
## B|16_intra|0.33|Atención y concentración sostenida ##
## B|17_intra|0.33|Atención y concentración sostenida ##
## B|20_intra|0.34|Atención y concentración sostenida ##
## B|34_intra|0.1|Autogestión del volumen y ritmo ##
## B|35_intra|0.6|Autogestión del volumen y ritmo ##
## B|36_intra|0.3|Autogestión del volumen y ritmo ##
## A|44_intra|0.1|Autogestión del volumen y ritmo ##
## A|45_intra|0.6|Autogestión del volumen y ritmo ##
## A|46_intra|0.3|Autogestión del volumen y ritmo ##
## A|18_intra|1|Carga de memoria ##
## B|18_intra|0.7|Carga de memoria ##
## B|19_intra|0.3|Carga de memoria ##
## B|22_intra|0.6|Ritmo de trabajo ##
## B|33_intra|0.3|Ritmo de trabajo ##
## B|37_intra|0.1|Ritmo de trabajo ##
## A|32_intra|0.6|Ritmo de trabajo ##
## A|43_intra|0.3|Ritmo de trabajo ##
## A|47_intra|0.1|Ritmo de trabajo ##
## A|20_intra|1|Simultaneidad ##
## A|13_intra|0.6|Volumen de trabajo ##
## A|14_intra|0.1|Volumen de trabajo ##
## A|15_intra|0.3|Volumen de trabajo ##
## B|13_intra|0.6|Volumen de trabajo ##
## B|14_intra|0.1|Volumen de trabajo ##
## B|15_intra|0.3|Volumen de trabajo ##
## B|23_intra|0.3|Descansos programados ##
## B|24_intra|0.7|Descansos programados ##
## A|33_intra|0.3|Descansos programados ##
## A|34_intra|0.7|Descansos programados ##
## B|21_intra|1|Trabajo nocturno ##
## A|31_intra|1|Trabajo nocturno ##
## B|38_intra|1|Claridad e impacto del cambio ##
## A|48_intra|0.3|Claridad e impacto del cambio ##
## A|49_intra|0.1|Claridad e impacto del cambio ##
## A|52_intra|0.6|Claridad e impacto del cambio ##
## B|39_intra|0.5|Participación en los cambios ##
## B|40_intra|0.5|Participación en los cambios ##
## A|50_intra|0.5|Participación en los cambios ##
## A|51_intra|0.5|Participación en los cambios ##
## B|79_intra|0.5|Cumplimiento en retribución ##
## B|80_intra|0.5|Cumplimiento en retribución ##
## A|96_intra|0.5|Cumplimiento en retribución ##
## A|97_intra|0.5|Cumplimiento en retribución ##
## A|100_intra|0.5|Desarrollo y movilidad interna ##
## A|99_intra|0.5|Desarrollo y movilidad interna ##
## B|82_intra|0.5|Desarrollo y movilidad interna ##
## B|83_intra|0.5|Desarrollo y movilidad interna ##
## A|102_intra|0.3|Estabilidad y confianza ##
## A|95_intra|0.7|Estabilidad y confianza ##
## B|85_intra|1|Estabilidad y confianza ##
## B|81_intra|1|Expectativa en la retribución ##
## A|98_intra|1|Expectativa en la retribución ##
## A|103_intra|0.3|Orgullo de pertenencia ##
## A|104_intra|0.5|Orgullo de pertenencia ##
## A|105_intra|0.2|Orgullo de pertenencia ##
## B|86_intra|0.3|Orgullo de pertenencia ##
## B|87_intra|0.5|Orgullo de pertenencia ##
## B|88_intra|0.2|Orgullo de pertenencia ##
## A|101_intra|1|Valoración del bienestar ##
## B|84_intra|1|Valoración del bienestar ##
## B|64_intra|0.3|Apoyo emocional ##
## B|73_intra|0.7|Apoyo emocional ##
## A|78_intra|0.3|Apoyo emocional ##
## A|89_intra|0.7|Apoyo emocional ##
## B|69_intra|0.25|Apoyo social e instrumental ##
## B|70_intra|0.25|Apoyo social e instrumental ##
## B|71_intra|0.25|Apoyo social e instrumental ##
## B|72_intra|0.25|Apoyo social e instrumental ##
## A|85_intra|0.25|Apoyo social e instrumental ##
## A|86_intra|0.25|Apoyo social e instrumental ##
## A|87_intra|0.25|Apoyo social e instrumental ##
## A|88_intra|0.25|Apoyo social e instrumental ##
## B|62_intra|0.33|Cohesión y sentido de pertenencia ##
## B|65_intra|0.33|Cohesión y sentido de pertenencia ##
## B|68_intra|0.34|Cohesión y sentido de pertenencia ##
## A|76_intra|0.2|Cohesión y sentido de pertenencia ##
## A|79_intra|0.2|Cohesión y sentido de pertenencia ##
## A|82_intra|0.2|Cohesión y sentido de pertenencia ##
## A|83_intra|0.2|Cohesión y sentido de pertenencia ##
## A|84_intra|0.2|Cohesión y sentido de pertenencia ##
## B|63_intra|0.30|Convivencia y respeto ##
## B|66_intra|0.6|Convivencia y respeto ##
## B|67_intra|0.1|Convivencia y respeto ##
## A|77_intra|0.30|Convivencia y respeto ##
## A|80_intra|0.6|Convivencia y respeto ##
## A|81_intra|0.1|Convivencia y respeto ##
## B|56_intra|0.05|Apoyo social y seguridad psicológica ##
## B|58_intra|0.4|Apoyo social y seguridad psicológica ##
## B|59_intra|0.2|Apoyo social y seguridad psicológica ##
## B|60_intra|0.2|Apoyo social y seguridad psicológica ##
## B|61_intra|0.15|Apoyo social y seguridad psicológica ##
## A|71_intra|0.05|Apoyo social y seguridad psicológica ##
## A|73_intra|0.4|Apoyo social y seguridad psicológica ##
## A|74_intra|0.3|Apoyo social y seguridad psicológica ##
## A|75_intra|0.25|Apoyo social y seguridad psicológica ##
## B|76_intra|0.5|Calidad y utilidad de la retroalimentación ##
## B|77_intra|0.5|Calidad y utilidad de la retroalimentación ##
## A|92_intra|0.5|Calidad y utilidad de la retroalimentación ##
## A|93_intra|0.5|Calidad y utilidad de la retroalimentación ##
## B|53_intra|1|Claridad operativa y flujo de información ##
## A|63_intra|0.5|Claridad operativa y flujo de información ##
## A|68_intra|0.5|Claridad operativa y flujo de información ##
## A|116_intra|0.1|Estresores para los lideres en su gestión de equipos ##
## A|117_intra|0.1|Estresores para los lideres en su gestión de equipos ##
## A|120_intra|0.1|Estresores para los lideres en su gestión de equipos ##
## A|125_intra|0.1|Estresores para los lideres en su gestión de equipos ##
## A|118_intra|0.1|Estresores para los lideres en su gestión de equipos ##
## A|122_intra|0.1|Estresores para los lideres en su gestión de equipos ##
## A|119_intra|0.1|Estresores para los lideres en su gestión de equipos ##
## A|121_intra|0.1|Estresores para los lideres en su gestión de equipos ##
## A|123_intra|0.1|Estresores para los lideres en su gestión de equipos ##
## A|124_intra|0.1|Estresores para los lideres en su gestión de equipos ##
## B|50_intra|0.34|Desarrollo, participación e inspiración ##
## B|51_intra|0.33|Desarrollo, participación e inspiración ##
## B|55_intra|0.33|Desarrollo, participación e inspiración ##
## A|65_intra|0.34|Desarrollo, participación e inspiración ##
## A|66_intra|0.33|Desarrollo, participación e inspiración ##
## A|70_intra|0.33|Desarrollo, participación e inspiración ##
## B|74_intra|1|Reconocimiento y refuerzo positivo ##
## A|90_intra|1|Reconocimiento y refuerzo positivo ##
## B|75_intra|0.3|Retroalimentación correctiva oportuna ##
## B|78_intra|0.7|Retroalimentación correctiva oportuna ##
## A|91_intra|0.3|Retroalimentación correctiva oportuna ##
## A|94_intra|0.7|Retroalimentación correctiva oportuna ##
## B|49_intra|0.3|Soporte instrumental y facilitación de la tarea ##
## B|52_intra|0.3|Soporte instrumental y facilitación de la tarea ##
## B|54_intra|0.1|Soporte instrumental y facilitación de la tarea ##
## B|57_intra|0.3|Soporte instrumental y facilitación de la tarea ##
## A|64_intra|0.3|Soporte instrumental y facilitación de la tarea ##
## A|67_intra|0.3|Soporte instrumental y facilitación de la tarea ##
## A|69_intra|0.1|Soporte instrumental y facilitación de la tarea ##
## A|72_intra|0.3|Soporte instrumental y facilitación de la tarea ##

## 3.2 Calificar indicadores en líneas de gestion con pesos relativos ##
## Antes de calificar la línea de gestion, se invierte el sentido del indicador cuando aplique (escala 0 a 1) con formula (1 – valor). Se detalla cuando aplica esta inversión con true, false. Se debe aplicar la fórmula de invertir de manera discriminada a las preguntas que aplican en cada indicador tanto para A como para B, como las que aplican solo para A o solo para B. Cuando se ha aplicado formula de inversión al indicador cuando aplique, según la agrupación de indicadores a líneas de gestión, se califica cada línea de gestion, aplicando promedio ponderado de los indicadores con peso relativo a la línea de gestion. En tabla se detalla el peso a aplicar en la ponderación. Se debe aplicar la formula de manera discriminada a los indicadores que aplican en linea tanto para A como para B, como las que aplican solo para A o solo para B. Cada trabajador obtiene un resultado en cada linea de gestion acorde a esta calificacion ponderada##

## |forma_intra| indicador| invertir_indicador|peso|linea_gestion|prioridad|##
## AyB|Autoeficacia percibida|FALSE|0.08|Afrontamiento del estrés y recursos psicológicos|Alta ##        
## AyB|Locus de control interno|FALSE|0.08|Afrontamiento del estrés y recursos psicológicos|Alta ##
## AyB|Evitación conductual|TRUE|0.06|Afrontamiento del estrés y recursos psicológicos|Alta ##
## AyB|Actitud resiliente|FALSE|0.05|Afrontamiento del estrés y recursos psicológicos|Alta ##
## AyB|Autonegación|TRUE|0.05|Afrontamiento del estrés y recursos psicológicos|Alta ##
## AyB|Convicción por las metas|FALSE|0.05|Afrontamiento del estrés y recursos psicológicos|Media ##
## AyB|Dedicación|FALSE|0.05|Afrontamiento del estrés y recursos psicológicos|Media ##
## AyB|Esfuerzo y dedicación|FALSE|0.05|Afrontamiento del estrés y recursos psicológicos|Media ##
## AyB|Esperanza del futuro|FALSE|0.05|Afrontamiento del estrés y recursos psicológicos|Media ##
## AyB|Firmeza y perseverancia|FALSE|0.05|Afrontamiento del estrés y recursos psicológicos|Media ##
## AyB|Orientación a la acción|FALSE|0.05|Afrontamiento del estrés y recursos psicológicos|Media ##
## AyB|Planificación|FALSE|0.05|Afrontamiento del estrés y recursos psicológicos|Media ##
## AyB|Renuncia|TRUE|0.05|Afrontamiento del estrés y recursos psicológicos|Media ##
## AyB|Resolutividad|FALSE|0.05|Afrontamiento del estrés y recursos psicológicos|Media ##
## AyB|Superación de adversidades|FALSE|0.05|Afrontamiento del estrés y recursos psicológicos|Media ##
## AyB|Activa red de apoyo|FALSE|0.02|Afrontamiento del estrés y recursos psicológicos|Baja ##
## AyB|Aprendizaje resiliente|FALSE|0.02|Afrontamiento del estrés y recursos psicológicos|Baja ##
## AyB|Busqueda de Apoyo profesional|FALSE|0.02|Afrontamiento del estrés y recursos psicológicos|Baja ##
## AyB|Busqueda de consejo y orientación|FALSE|0.02|Afrontamiento del estrés y recursos psicológicos|Baja ##
## AyB|Busqueda de soporte emocional|FALSE|0.02|Afrontamiento del estrés y recursos psicológicos|Baja ##
## AyB|Evitación cognitiva|TRUE|0.02|Afrontamiento del estrés y recursos psicológicos|Baja ##
## AyB|Optimismo del porvenir|FALSE|0.02|Afrontamiento del estrés y recursos psicológicos|Baja ##
## AyB|Propositividad|FALSE|0.02|Afrontamiento del estrés y recursos psicológicos|Baja ##
## AyB|Sensación de logro|FALSE|0.02|Afrontamiento del estrés y recursos psicológicos|Baja ##
## AyB|Alteraciones cognitivas|TRUE|1|Bienestar cognitivo|Media ##
## AyB|Desgaste emocional|TRUE|0.4|Bienestar emocional y trascendente|Alta ##
## AyB|Pérdida de sentido|TRUE|0.6|Bienestar emocional y trascendente|Alta ##
## AyB|Bienestar financiero|FALSE|1|Bienestar financiero|Alta ##
## AyB|Somatización y fatiga física|TRUE|1|Bienestar físico|Alta ##
## AyB|Apoyo social|FALSE|0.4|Bienestar extralaboral|Alta ##
## AyB|Dererioro de relaciones sociales y aislamiento|TRUE|0.2|Bienestar extralaboral|Alta ##
## AyB|Relación con el núcleo familiar|FALSE|0.2|Bienestar extralaboral|Media ##
## AyB|Accesibilidad del entorno|FALSE|0.05|Bienestar extralaboral|Baja ##
## AyB|Condiciones de la vivienda|FALSE|0.05|Bienestar extralaboral|Baja ##
## AyB|Movilidad eficiente y desplazamiento saludable|FALSE|0.05|Bienestar extralaboral|Baja ##
## AyB|Seguridad del entorno|FALSE|0.05|Bienestar extralaboral|Baja ##
## AyB|Desmotivación y desgaste laboral|TRUE|1|Motivación laboral|Media ##
## AyB|Interferencia temporal|TRUE|0.5|Bienestar vida-trabajo|Alta ##
## AyB|Impacto cognitivo-relacional|TRUE|0.2|Bienestar vida-trabajo|Media ##
## AyB|Equilibrio y recuperación|FALSE|0.2|Bienestar vida-trabajo|Media ##
## AyB|Conflicto vida - trabajo|TRUE|0.1|Bienestar vida-trabajo|Baja ##
## AyB|Conductas de riesgo|TRUE|1|Comportamientos seguros|Baja ##
## B|Propósito del rol y redes de soporte|FALSE|0.5|Arquitectura de roles y responsabilidades|Alta ##
## B|Claridad en funciones y objetivos del rol|FALSE|0.3|Arquitectura de roles y responsabilidades|Media ##
## B|Autonomía decisional|FALSE|0.2|Arquitectura de roles y responsabilidades|Media ##
## A|Autonomía decisional|FALSE|0.15|Arquitectura de roles y responsabilidades|Alta ##
## A|Integridad normativa|FALSE|0.2|Arquitectura de roles y responsabilidades|Alta ##
## A|Contradicción en estructura de mando|TRUE|0.15|Arquitectura de roles y responsabilidades|Alta ##
## A|Claridad en funciones y objetivos del rol|FALSE|0.1|Arquitectura de roles y responsabilidades|Media ##
## A|Decisiones críticas y resultados de gestión|TRUE|0.1|Arquitectura de roles y responsabilidades|Media ##
## A|Responsabilidad por bienes y valores|TRUE|0.1|Arquitectura de roles y responsabilidades|Media ##
## A|Pertinencia operativa|FALSE|0.05|Arquitectura de roles y responsabilidades|Baja ##
## A|Propósito del rol y redes de soporte|FALSE|0.1|Arquitectura de roles y responsabilidades|Baja ##
## A|Responsabilidad por la integridad y salud de otros|TRUE|0.05|Arquitectura de roles y responsabilidades|Baja ##
## AyB|Alineación de competencias y ajuste al cargo|FALSE|0.4|Aprendizaje y desarrollo (L&D Strategy)|Alta ##
## AyB|Pertinencia y aplicabilidad de la capacitación|FALSE|0.3|Aprendizaje y desarrollo (L&D Strategy)|Media ##
## AyB|Rol enriquecido|FALSE|0.2|Aprendizaje y desarrollo (L&D Strategy)|Baja ##
## AyB|Acceso a capacitación|FALSE|0.1|Aprendizaje y desarrollo (L&D Strategy)|Baja ##
## AyB|Disonancia emocional|TRUE|0.6|Condiciones emocionales saludables|Alta ##
## AyB|Hostilidad y violencia|TRUE|0.3|Condiciones emocionales saludables|Alta ##
## AyB|Exposición al sufrimiento|TRUE|0.1|Condiciones emocionales saludables|Media ##
## AyB|Riesgos biomecánicos|TRUE|0.4|Condiciones físicas saludables|Alta ##
## AyB|Confort del entorno|FALSE|0.3|Condiciones físicas saludables|Media ##
## AyB|Orden percibido|FALSE|0.05|Condiciones físicas saludables|Baja ##
## AyB|Riesgos higiénicos|TRUE|0.05|Condiciones físicas saludables|Baja ##
## AyB|Seguridad percibida|FALSE|0.2|Condiciones físicas saludables|Baja ##
## B|Ritmo de trabajo|TRUE|0.45|Cargas de trabajo saludables|Alta ##
## B|Volumen de trabajo|TRUE|0.25|Cargas de trabajo saludables|Alta ##
## B|Autogestión del volumen y ritmo|FALSE|0.1|Cargas de trabajo saludables|Media ##
## B|Atención y concentración sostenida|TRUE|0.15|Cargas de trabajo saludables|Media ##
## B|Carga de memoria|TRUE|0.05|Cargas de trabajo saludables|Baja ##
## A|Ritmo de trabajo|TRUE|0.3|Cargas de trabajo saludables|Alta ##
## A|Volumen de trabajo|TRUE|0.25|Cargas de trabajo saludables|Alta ##
## A|Simultaneidad|TRUE|0.25|Cargas de trabajo saludables|Alta ##
## A|Atención y concentración sostenida|TRUE|0.05|Cargas de trabajo saludables|Media ##
## A|Autogestión del volumen y ritmo|FALSE|0.1|Cargas de trabajo saludables|Baja ##
## A|Carga de memoria|TRUE|0.05|Cargas de trabajo saludables|Baja ##
## AyB|Trabajo nocturno|TRUE|0.7|Jornadas de trabajo saludables|Alta ##
## AyB|Descansos programados|FALSE|0.3|Jornadas de trabajo saludables|Media ##
## AyB|Claridad e impacto del cambio|FALSE|0.3|Cambio organizacional|Baja ##
## AyB|Participación en los cambios|FALSE|0.7|Cambio organizacional|Baja ##
## AyB|Orgullo de pertenencia|FALSE|0.4|Engagement organizacional|Alta ##
## AyB|Estabilidad y confianza|FALSE|0.2|Engagement organizacional|Media ##
## AyB|Desarrollo y movilidad interna|FALSE|0.15|Engagement organizacional|Media ##
## AyB|Expectativa en la retribucion|FALSE|0.05|Engagement organizacional|Baja ##
## AyB|Valoración del bienestar|FALSE|0.1|Engagement organizacional|Baja ##
## AyB|Cumplimiento en retribución|FALSE|0.1|Engagement organizacional|Baja ##
## AyB|Convivencia y respeto|FALSE|0.5|Convivencia y relacionamiento|Alta ##
## AyB|Apoyo social e instrumental|FALSE|0.2|Convivencia y relacionamiento|Media ##
## AyB|Apoyo emocional|FALSE|0.2|Convivencia y relacionamiento|Media ##
## AyB|Cohesión y sentido de pertenencia|FALSE|0.1|Convivencia y relacionamiento|Baja ##
## B|Apoyo social y seguridad psicológica|FALSE|0.3|Ecosistema de liderazgo con impacto psicosocial|Alta ##
## B|Soporte instrumental y facilitación de la tarea|FALSE|0.2|Ecosistema de liderazgo con impacto psicosocial|Alta ##
## B|Claridad operativa y flujo de información|FALSE|0.2|Ecosistema de liderazgo con impacto psicosocial|Media ##
## B|Calidad y utilidad de la retroalimentación|FALSE|0.1|Ecosistema de liderazgo con impacto psicosocial|Media ##
## B|Desarrollo, participación e inspiración|FALSE|0.1|Ecosistema de liderazgo con impacto psicosocial|Media ##
## B|Reconocimiento y refuerzo positivo|FALSE|0.05|Ecosistema de liderazgo con impacto psicosocial|Baja ##
## B|Retroalimentación correctiva oportuna|FALSE|0.05|Ecosistema de liderazgo con impacto psicosocial|Baja ##
## A|Apoyo social y seguridad psicológica|FALSE|0.2|Ecosistema de liderazgo con impacto psicosocial|Alta ##
## A|Claridad operativa y flujo de información|FALSE|0.2|Ecosistema de liderazgo con impacto psicosocial|Alta ##
## A|Estresores para los lideres en su gestión de equipos|FALSE|0.2|Ecosistema de liderazgo con impacto psicosocial|Alta ##
## A|Soporte instrumental y facilitación de la tarea|FALSE|0.15|Ecosistema de liderazgo con impacto psicosocial|Media ##
## A|Calidad y utilidad de la retroalimentación|FALSE|0.1|Ecosistema de liderazgo con impacto psicosocial|Media ##
## A|Desarrollo, participación e inspiración|FALSE|0.05|Ecosistema de liderazgo con impacto psicosocial|Baja ##
## A|Reconocimiento y refuerzo positivo|FALSE|0.05|Ecosistema de liderazgo con impacto psicosocial|Baja ##
## A|Retroalimentación correctiva oportuna|FALSE|0.05|Ecosistema de liderazgo con impacto psicosocial|Baja ##

## 3.3 Calificar ejes de gestión con pesos relativos ##
## En este caso, no debe invertirse el sentido, se califica según la agrupación de líneas de gestión en ejes, se califica cada eje, aplicando promedio ponderado de las líneas de gestión con peso relativo al eje. En la tabla se detalla el peso a aplicar en la ponderación. Cada trabajador obtiene resultado de cada eje de gestion segun calificacion descrita##

## |linea_gestion| peso| eje_gestion|prioridad ##
## Afrontamiento del estrés y recursos psicológicos|0.15|Bienestar biopsicosocial|Alta
## Bienestar cognitivo|0.05|Bienestar biopsicosocial|Media
## Bienestar emocional y trascendente|0.2|Bienestar biopsicosocial|Alta
## Bienestar extralaboral|0.1|Bienestar biopsicosocial|Media
## Bienestar financiero|0.15|Bienestar biopsicosocial|Alta
## Bienestar físico|0.1|Bienestar biopsicosocial|Alta
## Bienestar vida-trabajo|0.1|Bienestar biopsicosocial|Media
## Motivación laboral|0.05|Bienestar biopsicosocial|Media
## Comportamientos seguros|0.05|Bienestar biopsicosocial|Bajo
## Arquitectura de roles y responsabilidades|0.15|Condiciones de trabajo saludable|Media
## Aprendizaje y desarrollo (L&D Strategy)|0.05|Condiciones de trabajo saludable|Bajo
## Condiciones emocionales saludables|0.15|Condiciones de trabajo saludable|Alta
## Condiciones físicas saludables|0.1|Condiciones de trabajo saludable|Media
## Carga de trabajo saludable|0.25|Condiciones de trabajo saludable|Alta
## Jornadas de trabajo saludables|0.15|Condiciones de trabajo saludable|Alta
## Cambio organizacional|0.05|Condiciones de trabajo saludable|Bajo
## Engagement organizacional|0.1|Condiciones de trabajo saludable|Media
## Convivencia y relacionamiento|0.4|Entorno y clima de trabajo saludable|Alta
## Ecosistema de liderazgo con impacto psicosocial|0.6|Entorno y clima de trabajo saludable|Alta

## Paso 4.Calificar según puntos de corte los ejes y lineas de gestión, generando una interpretación categorica del nivel## 
## 4.1 Se califica los ejes y lineas de gestion, generando los puntajes que salen del paso anterior e interpretandolos con las siguientes tablas. Se deben crear los rangos aplicando los puntos de corte y estableciendo los 5 niveles de de gestion.Notese que la escala viene de mayor a menor, a menor resultado intervencion mas critica. Cada trabajador obtiene puntaje transformado e interpretacion del nivel de gestion en cada eje e indicador##

## |forma_intra| eje_gestion| gestion_prorrogable_max|gestion_preventiva_max|gestion_mejoraselectiva_max|intervencion_inmediata_max|intervencion_urgente_max|##
## AyB|Bienestar biopsicosocial|1|0.79|0.65|0.45|0.29
## AyB|Condiciones de trabajo saludable|1|0.79|0.65|0.45|0.29
## AyB|Entorno y clima de trabajo saludable|1|0.79|0.65|0.45|0.29

## |forma_intra| linea_gestion| gestion_prorrogable_max|gestion_preventiva_max|gestion_mejoraselectiva_max|intervencion_inmediata_max|intervencion_urgente_max|##
## AyB|Afrontamiento del estrés y recursos psicológicos|1|0.79|0.65|0.45|0.29
## AyB|Bienestar cognitivo|1|0.79|0.65|0.45|0.29
## AyB|Bienestar emocional y trascendente|1|0.79|0.65|0.45|0.29
## AyB|Bienestar financiero|1|0.79|0.65|0.45|0.29
## AyB|Bienestar físico|1|0.79|0.65|0.45|0.29
## AyB|Bienestar extralaboral|1|0.79|0.65|0.45|0.29
## AyB|Motivación laboral|1|0.79|0.65|0.45|0.29
## AyB|Bienestar vida-trabajo|1|0.79|0.65|0.45|0.29
## AyB|Comportamientos seguros|1|0.79|0.65|0.45|0.29
## AyB|Arquitectura de roles y responsabilidades|1|0.79|0.65|0.45|0.29
## AyB|Aprendizaje y desarrollo (L&D Strategy)|1|0.79|0.65|0.45|0.29
## AyB|Condiciones emocionales saludables|1|0.79|0.65|0.45|0.29
## AyB|Condiciones físicas saludables|1|0.79|0.65|0.45|0.29
## AyB|Carga de trabajo saludable|1|0.79|0.65|0.45|0.29
## AyB|Jornadas de trabajo saludables|1|0.79|0.65|0.45|0.29
## AyB|Cambio organizacional|1|0.79|0.65|0.45|0.29
## AyB|Engagement organizacional|1|0.79|0.65|0.45|0.29
## AyB|Convivencia y relacionamiento|1|0.79|0.65|0.45|0.29
## AyB|Ecosistema de liderazgo con impacto psicosocial|1|0.79|0.65|0.45|0.29

## 4.2 Se asigna una interpretacion global a cada nivel. Se debe poder hacer un conteo de trabajadores en cada nivel de gestion, eje e indicador##
## nivel_gestion|etiqueta_gestion|enfoque_gestion ## (*** utilizar en storytelling del visualizador **)
## Gestión prorrogable|Promoción|Reforzar y mantener factores protectores y controles actuales. Implementar actividades de promoción de la salud y el bienestar de manera transversal a la organización, para reconocer las fortalezas y mantener las acciones actuales.
## Gestión preventiva|Educación y prevención|Actuar desde actividades de formación y capacitación que permitan desarrollar pautas de autocuidado y reconocimiento de acciones tempranas para prevenir los riesgos y peligros desde las personas y la organización. Prevenir retos futuros y generar seguimiento para proactivo 
## Gestión de mejora selectiva|Ajuste y mejora|Intervención en indicadores e dimensiones específicas que puntuaron en niveles altos con el fin de mejorar e impactar focos de riesgo y evitar que la magnitud aumente en ausencia de controles.
## Intervención correctiva|Control correctivo|Implementación de intervenciones mediante protocolos estructurados para mitigar o mejorar los factores de riesgo identificados. Articular estas intervenciones en el SVE psicosocial (Sistemas de Vigilancia Epidemiológica).
## Intervención Urgente|Intervención urgente|Intervención que debería implementarse dentro de los 6 meses siguientes, la cual aborda la implementación de los protocolos sobre las lineas de alta prioridad, desde una perspectiva organizacional, grupal e individual y con conexión directa al PVE de la empresa.

## 4.3 Se establece una relacion entre linea de gestion y protocolos de gestion.  Cada linea de gestion tiene asociado unos protocolos que tienen a su vez un objetivo y resultado esperado en KPI. Se debe poder generar un reporte por protocolo que establezca la linea base del KPI, el objetivo y resultado esperado de la implementación del protocolo. Asimismo, este es insumo para enviar Json a sistema RAG de protocolos##
## eje_gestion|linea_gestion|prot_id|protocolo_nombre|objetivo|resultado_esperado ##

## Bienestar biopsicosocial|	Afrontamiento del estrés y recursos psicológicos|	PROT-01|	Afrontamiento del estrés y recursos psicológicos|	Fortalecer los recursos psicológicos del trabajador para el manejo adaptativo del estrés laboral|	 KPI: Índice de Afrontamiento Eficaz del Estrés (IAEE) 
## Bienestar biopsicosocial|	Bienestar emocional y trascendente|	PROT-03|	Bienestar emocional y trascendente — Promoción salud mental positiva|	Promover la salud mental positiva, el sentido de vida y el florecimiento en el entorno laboral|	 KPI: Índice de Bienestar Emocional y Trascendente (IBET) 
## Bienestar biopsicosocial|	Bienestar emocional y trascendente|	PROT-04|	Bienestar emocional y trascendente — Intervención en trastornos|	Prevenir, detectar tempranamente e intervenir el burnout, depresión y ansiedad ocupacional|	 KPI: Índice de Bienestar Emocional y Trascendente (IBET) 
## Bienestar biopsicosocial|	Bienestar emocional y trascendente|	PROT-05|	Bienestar emocional y trascendente — Crisis, duelo y emergencia|	Responder eficazmente ante crisis agudas, trauma, duelo y emergencias de salud mental|	 KPI: Índice de Bienestar Emocional y Trascendente (IBET) 
## Bienestar biopsicosocial|	Bienestar financiero|	PROT-07|	Bienestar financiero|	Reducir el estrés financiero mediante educación y orientación económica en el trabajador|	 KPI: Índice de Bienestar Financiero del Trabajador (IBFT) 
## Bienestar biopsicosocial|	Bienestar físico|	PROT-08|	Bienestar físico y hábitos saludables|	Promover hábitos saludables físicos: actividad, nutrición, ergonomía postural y sueño|	 KPI: Índice de Bienestar Físico y Hábitos Saludables (IBFHS) 
## Bienestar biopsicosocial|	Bienestar extralaboral|	PROT-06|	Bienestar extralaboral|	Apoyar el bienestar en las dimensiones no laborales: familia, ocio, red social y espiritualidad|	 KPI: Índice de Bienestar Extralaboral (IBE) 
## Bienestar biopsicosocial|	Bienestar vida-trabajo|	PROT-09|	Bienestar vida-trabajo (conciliación trabajo-familia)|	Facilitar la conciliación trabajo-familia y la flexibilidad como factor protector psicosocial|	 KPI: Índice de Conciliación Vida-Trabajo (ICVT) 
## Bienestar biopsicosocial|	Motivación laboral|	PROT-20|	Gestión del Engagement organizacional|	Construir y sostener el compromiso organizacional y la motivación del trabajador|	 KPI: Índice de Motivación Laboral (IML) 
## Bienestar biopsicosocial|	Bienestar cognitivo|	PROT-02|	Bienestar cognitivo|	Preservar y potenciar las funciones cognitivas que sostienen el desempeño y bienestar mental|	 KPI: Índice de Bienestar Cognitivo (IBC) 
## Bienestar biopsicosocial|	Comportamientos seguros|	PROT-10|	Prevención de conductas de riesgo|	Prevenir conductas de riesgo: consumo de SPA, violencia de género y comportamientos disfuncionales|	 KPI: Tasa de Comportamientos seguros (TCR) 
## Condiciones de trabajo saludable|	Condiciones emocionales saludables|	PROT-13|	Gestión de condiciones emocionales — Demandas emocionales|	Gestionar las demandas emocionales del trabajo y prevenir el desgaste empático|	 KPI: Índice de Gestión de Demandas Emocionales (IGDE) 
## Condiciones de trabajo saludable|	Carga de trabajo saludable|	PROT-15|	Gestión de la carga de trabajo|	Equilibrar la carga de trabajo previniendo la sobrecarga como fuente de riesgo psicosocial|	 KPI: Índice de Carga de Trabajo Saludable (ICTS) 
## Condiciones de trabajo saludable|	Jornadas de trabajo saludables|	PROT-16|	Gestión de jornadas de trabajo|	Regular las jornadas laborales para proteger el descanso, desconexión y salud mental|	 KPI: Índice de Jornadas de Trabajo Saludables (IJTS) 
## Condiciones de trabajo saludable|	Arquitectura de roles y responsabilidades|	PROT-11|	Arquitectura de roles y responsabilidades|	Diseñar roles, cargas y responsabilidades que prevengan el riesgo psicosocial de origen laboral|	 KPI: Índice de Claridad de Roles y Responsabilidades (ICRR) 
## Condiciones de trabajo saludable|	Condiciones físicas saludables|	PROT-14|	Gestión de condiciones físicas y ergonomía|	Controlar los riesgos físicos, ergonómicos y ambientales que afectan la salud mental|	 KPI: Índice de Condiciones Físicas Saludables (ICFS) 
## Condiciones de trabajo saludable|	Engagement organizacional|	PROT-20|	Gestión del Engagement organizacional|	Construir y sostener el compromiso organizacional y la motivación del trabajador|	 KPI: Índice de Engagement Organizacional (IEO) 
## Condiciones de trabajo saludable|	Aprendizaje y desarrollo (L&D Strategy)|	PROT-12|	Estrategia de aprendizaje y desarrollo (L&D Strategy)|	Desarrollar competencias, conocimiento y aprendizaje como factores protectores del bienestar|	 KPI: Índice de Desarrollo de Competencias y Aprendizaje (IDCA) 
## Condiciones de trabajo saludable|	Cambio organizacional|	PROT-17|	Gestión del cambio organizacional|	Gestionar los procesos de cambio organizacional minimizando el impacto psicosocial|	 KPI: Índice de Gestión Saludable del Cambio Organizacional (IGSCO) 
## Entorno y clima de trabajo saludable|	Ecosistema de liderazgo con impacto psicosocial|	PROT-19|	Ecosistema de liderazgo con impacto psicosocial|	Desarrollar un ecosistema de liderazgo que genere impacto psicosocial positivo|	 KPI: Índice de Liderazgo con Impacto Psicosocial Positivo (ILIPP) 
## Entorno y clima de trabajo saludable|	Convivencia y relacionamiento|	PROT-18|	Convivencia, relacionamiento y prevención de violencia|	Prevenir y gestionar el acoso, la violencia y los conflictos interpersonales en el trabajo|	 KPI: Tasa de convivencia y respeto (TCLC) 

## Paso 5: Se referencia segun el sector económico de la empresa, cuáles son los 3 protocolos de mayor exigencia legal y técnica para su sector segun tabla de abajo. Esto se compara a la prioridad de los resultados obtenidos. Se listan los protocolos con mayor exigencia legal actualmente. Se debe poder comparar la empresa los protocolos prioritarios segun sector y los protocolos que son mas criticos segun sus resultados. Se priorizan los protocolos que debe implementar la empresa en orden de jerarquía, segun el % de trabajadores tanto a como b que se encuentran en prioridad inmediata o urgente. Se genera un reporte de priorización de protocolos##

## Los sectores económicos tienen un orden sugerido de activación de los protocolos##:

## | Sector | Protocolos en orden de implementacion segun prioridad|##
## |--------|------------------------------------------------------|##
## | Salud | PROT-04, PROT-05, PROT-13, PROT-10 |##
## | Educación | PROT-18, PROT-13, PROT-02, PROT-11 |##
## | Admón. Pública | PROT-17, PROT-11, PROT-19, PROT-20 |##
## | Comercio/Financiero | PROT-09, PROT-15, PROT-20, PROT-07 |##
## | Construcción | PROT-08, PROT-14, PROT-16, PROT-10 |##
## | Servicios | PROT-09, PROT-18, PROT-13, PROT-03 |##
## | Manufactura | PROT-14, PROT-08, PROT-15, PROT-16 |##
## | Transporte | PROT-16, PROT-08, PROT-14, PROT-09 |##
## | Minas/Energía | PROT-08, PROT-14, PROT-10, PROT-05 |##
## | Agricultura | PROT-08, PROT-06, PROT-07, PROT-16 |##

## Paso 6: Se debe generar un insumo para el programa de vigilancia epidemiológica, el cual hace un consolidado por cada uno de los 11 indicadores priorizados para vigilancia epidemiológica, el total de trabajadores que cumplen con el criterio de caso sospechoso. Identifica el listado de trabajadores que cumplen algun criterio, especificando cual criterio de caso sospechoso cumplen. Este analisis busca en fact_tables instrumentos intralaboral a y b, estres, extralaboral y en dimensional_tables: ausentismo_12 meses. Se hace un conteo de cuantos criterios de casos sospechoso cumple cada trabajador, se ordenan los trabajadores de mayor numero de criterios a menor. La siguiente tabla especifica los indicadores y criterios. El sistema debe mostrar esta misma tabla resumen con: Fuente de información, Indicador,Definición de caso probable,	Criterio caso sospechoso, # de trabajadores que cumplen el criterio de caso sospechoso,	Criterio caso confirmado, Mecanismo de confirmación,	Soporte legal,	Enfoque. Además de la tabla resumen, el sistema identifica todos los trabajadores especificando cuales criterios de caso sospechoso cumplen y ordenando de mayor a menor los trabajadores con cumplimiento de más # de indicadores ##

## | Fuentes de información	Indicador|	Definición de caso probable|	Criterio caso sospechoso|	Criterio caso confirmado|	Mecanismo de confirmación|	Soporte legal|	Enfoque |##

# intralaboral A_o_B|	Convivencia y respeto|	Trabajadores con autoreporte de exposición a conductas de maltrato o irrespeto en el entorno laboral|	Score<=0.45|	Trabajadores con queja de acoso laboral en comité de convivencia laboral|	Reportes oficiales al CCL|	Res.2764/2022/Ley 1010/2006	|Cuidado de la salud ## 
## estres|	Conductas de riesgo|	Trabajadores con autoreporte de comportamientos de riesgo como consumo de drogas, bebidas, accidentes|	Score>=0.45|	Trabajadores con consumo problemático de alcohol  y sustancias psicoactivas|	Pruebas biológicas + pruebas de patrón de consumo|	Res.2764/2022/Res/1968 de 2025/Dec 728/2025+Res/PESV|	Cuidado de la salud ##
## ausentismo|	Tipo de ausentismo-Accidente de trabajo|	Trabajadores con registro de accidentalidad laboral en los últimos 12 meses|	> 1 evento de accidente laboral en tipo de ausentismo|	Trabajadores que han presentado accidentes de trabajo multiples, más de 2 accidentes en 12 meses|	Furat, formatos investigación, reportes|	Res.2764/2022|	Cuidado de la salud ##
## ausentismo|	Trabajadores con reportes de ausentismo de interés psicosocial|	Trabajadores que en los ultimos 12 meses han presentado al menos 1 evento de ausentismo de interés psicosocial|	1 o más eventos de ausentismo laboral de interés psicosocial diagnostico CIE|	Trabajadores que han presentado diagnósticos de interés relacionados con riesgo psicosocial en los ultimos 12 meses|	Registros de ausentismo laboral|	Res.2764/2022|	Cuidado de la salud ##
## estres|	Somatización y fatiga física|	Trabajadores con autoreporte de sintomas físicos asociados al estrés|	Score>=0.45|	Trabajadores que están atravezando un evento vital estresante o alta exposición a factores de riesgo psicosocial laboral|	Valoración psicosocial individual|	Res.2764/2022/Res. 2646/2008|	Cuidado de la salud ##
## estres|	Desgaste emocional|	Trabajadores con autoreporte de sintomas emocionales asociados al estrés|	Score>=0.45|	Trabajadores que están atravezando un evento vital estresante o alta exposición a factores de riesgo psicosocial laboral|	Valoración psicosocial individual|	Res.2764/2022/Res. 2646/2008|	Cuidado de la salud ##
## estres|	Desmotivación y desgaste laboral|	Trabajadores con autoreporte de desmotivación laboral, desgaste e intención de rotación|	Score>=0.45|	Sin requisito legal directo|	Valoración psicosocial individual|	Sin requisito legal directo|	Promoción del bienestar ##
## estres|	Pérdida de sentido|	Trabajadores con autoreporte de perdida de sentido  de vida|	Score>=0.45|	Sin requisito legal directo|	Valoración psicosocial individual|	Sin requisito legal directo|	Promoción del bienestar ##
## intralaboral A_o_B|	Interferencia temporal|	Trabajadores con autoreporte de alto conflicto del trabajo con la vida personal por interferencia de tiempo|	Score>=0.45|	Sin requisito legal directo	Valoración psicosocial individual|	Sin requisito legal directo|	Promoción del bienestar##
## extralaboral|	Bienestar financiero|	Trabajadores con autoreporte de estresores financieros y alto endeudamiento|	Score<=0.45|	Sin requisito legal directo|	Valoración psicosocial individual|	Sin requisito legal directo|	Promoción del bienestar
## extralaboral|Apoyo social|	Trabajadores con autoreporte de baja calidad en red de apoyo social	|Score<=0.45|	Sin requisito legal directo	|Valoración psicosocial individual|Sin requisito legal directo	|Promoción del bienestar## 

