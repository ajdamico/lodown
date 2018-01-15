get_catalog_pns <-
	function( data_name = "pns" , output_dir , ... ){

		catalog <- NULL
	
		year_listing <- readLines( textConnection( RCurl::getURL( "ftp://ftp.ibge.gov.br/PNS/" ) ) )

		ay <- rev( gsub( "(.*) (.*)" , "\\2" , year_listing ) )

		suppressWarnings( available_years <- ay[ as.numeric( ay ) %in% ay ] )
	
		for( this_year in available_years ){
			
			file_listing <- RCurl::getURL( paste0( "ftp://ftp.ibge.gov.br/PNS/" , this_year , "/microdados/" ) )

			af <- gsub( "(.*) (.*)" , "\\2" , file_listing )
			af <- gsub( "\\r\\n" , "" , af )
			af <- gsub( "\\n" , "" , af )

			catalog <-
				rbind( 
					catalog ,
					data.frame(
						year = this_year ,
						full_url = paste0( "ftp://ftp.ibge.gov.br/PNS/" , this_year , "/microdados/" , af ) ,
						output_folder = paste0( output_dir , "/" ) ,
						long_file = paste0( this_year , " long questionnaire survey.rds" ) , 
						all_file = paste0( this_year , " all questionnaire survey.rds" ) ,
						long_design = paste0( this_year , " long questionnaire survey design.rds" ) , 
						all_design = paste0( this_year , " all questionnaire survey design.rds" ) ,
						stringsAsFactors = FALSE
					)
				)
		
		}
		
		catalog

	}


lodown_pns <-
	function( data_name = "pns" , catalog , ... ){

		on.exit( print( catalog ) )

		tf <- tempfile()
		
		if( .Platform$OS.type != 'windows' ) {
			previous_encoding <- getOption( "encoding" )
			on.exit( options( encoding = previous_encoding ) )
			options( encoding = 'windows-1252' )
		}
		
		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )
			
			# identify household (domicilio) data file
			dd <- grep( "Dados/DOMPNS" , unzipped_files , value = TRUE )

			# identify person data file
			pd <- grep( "Dados/PESPNS" , unzipped_files , value = TRUE )

			# identify household (domicilio) sas import script
			ds <- grep( "DOMPNS(.*)\\.sas" , unzipped_files , value = TRUE )

			# identify person sas import script
			ps <- grep( "PESPNS(.*)\\.sas" , unzipped_files , value = TRUE )

			# create a data.frame object `dom` containing one record per household
			dom <- read_SAScii( dd , ds , na_values = "." )

			# create a data.frame object `pes` containing one record per person
			pes <- read_SAScii( pd , ps , na_values = "." )

			# convert all columns to lowercase
			names( dom ) <- tolower( names( dom ) )
			names( pes ) <- tolower( names( pes ) )

			# pre-stratified pes weight
			names( pes )[ names( pes ) == 'v0029' ] <- 'pre_pes_long'
			names( pes )[ names( pes ) == 'v0028' ] <- 'pre_pes_full'


			# merge dom and pes
			x <- merge( dom , pes , by = c( "v0001" , "v0024" , "upa_pns" , "v0006_pns" ) )

			stopifnot( nrow( x ) == nrow( pes ) )

			rm( dom , pes ) ; gc()

			# people with self evaluated health good or very good  
			x$saude_b_mb <- as.numeric( x$n001 %in% c( '1' , '2' ) )

			# urban / rural
			x$situ <- factor( substr( x$v0024 , 7 , 7 ) , labels = c( 'urbano' , 'rural' ) )

			# sex
			x$c006 <- factor( x$c006 , labels = c( 'masculino' , 'feminino' ) )

			# state names
			estado_names <- c( "Rondonia" , "Acre" , "Amazonas" , "Roraima" , "Para" , "Amapa" , "Tocantins" , "Maranhao" , "Piaui" , "Ceara" , "Rio Grande do Norte" , "Paraiba" , "Pernambuco" , "Alagoas" , "Sergipe" , "Bahia" , "Minas Gerais" , "Espirito Santo" , "Rio de Janeiro" , "Sao Paulo" , "Parana" , "Santa Catarina" , "Rio Grande do Sul" , "Mato Grosso do Sul" , "Mato Grosso" , "Goias" , "Distrito Federal" )
			
			x$uf <- factor( x$v0001 , labels = estado_names )

			# region
			x$region <- factor( substr( x$v0001 , 1 , 1 ) , labels = c( "Norte" , "Nordeste" , "Sudeste" , "Sul" , "Centro-Oeste" ) )
			

			# numeric recodes
			x[ , c( 'p04101' , 'p04102' , 'p04301' , 'p04302' ) ] <- sapply( x[ , c( 'p04101' , 'p04102' , 'p04301' , 'p04302' ) ] , as.numeric )


			# worker recodes
			x$tempo_desl_trab = ifelse( is.na( x$p04101 ) , 0 , x$p04101 * 60 + x$p04102 )
			x$tempo_desl_athab = ifelse( is.na( x$p04301 ) , 0 , x$p04301 * 60 + x$p04302 )
			
			
			x$tempo_desl <- x$tempo_desl_trab + x$tempo_desl_athab

			x$atfi04 <- as.numeric( x$tempo_desl >= 30 )


			# categorical age 
			x$age_cat <- factor( 1 + findInterval( as.numeric( x$c008 ) , c( 18 , 30 , 40 , 60 ) ) , labels = c( "0-17" , "18-29" , "30-39" , "40-59" , "60+" ) )

			# race
			x$raca <- as.numeric( x$c009 )
			x[ x$raca == 9 , 'raca' ] <- NA
			x$raca <- factor( x$raca , labels = c( 'Branca' , 'Preta' , 'Amarela' , 'Parda' , 'Indigena' ) )

			# education
			x$educ <- factor( 1 + findInterval( as.numeric( x$vdd004 ) , c( 3 , 5 , 7 ) ) , labels = c( "SinstFundi" , "FundcMedi" , "MedcSupi" , "Supc" ) )

			# number of people in the household
			x$c001 <- as.numeric(x$c001)

			# column of all ones
			x$one <- 1                   



			# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
			# design object for people answering the long questionnaire #
			pes_sel <- x[ x$m001 == "1" , ]

			# pre-stratified design
			pes_sel_des <-
				survey::svydesign(
					id = ~ upa_pns ,
					strata = ~ v0024 ,
					data = pes_sel ,
					weights = ~ pre_pes_long ,
					nest = TRUE
				)

			# figure out stratification targets
			post_pop <- unique( pes_sel[ c( 'v00293.y' , 'v00292.y' ) ] )

			names( post_pop ) <- c( "v00293.y" , "Freq" )

			# post-stratified design
			pes_sel_des_pos <- survey::postStratify( pes_sel_des , ~v00293.y , post_pop )

			# save the long questionnaire survey design
			saveRDS( pes_sel , file = paste0( catalog[ i , 'output_folder' ] , catalog[ i , 'long_file' ] ) , compress = FALSE )
			saveRDS( pes_sel_des_pos , file = paste0( catalog[ i , 'output_folder' ] , catalog[ i , 'long_design' ] ) , compress = FALSE )

			# final design object for people answering the long questionnaire #
			# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


			# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
			# design object for people answering only short or long questionnaire #

			# pre-stratified design object for all people

			pes_all_des <-
				survey::svydesign(
					id = ~ upa_pns ,
					strata = ~ v0024 , 
					data = x , 
					weights = ~ pre_pes_full , 
					nest = TRUE
				)

			# figure out stratification targets
			post_pop_all <- unique( x[ , c( 'v00283.y' , 'v00282.y' ) ] )

			names( post_pop_all ) <- c( "v00283.y" , "Freq" )


			# post-stratified design
			pes_all_des_pos <- survey::postStratify( pes_all_des , ~ v00283.y , post_pop_all )

			# save the all-respondent questionnaire survey and design
			saveRDS( x , file = paste0( catalog[ i , 'output_folder' ] , catalog[ i , 'all_file' ] )  , compress = FALSE )
			saveRDS( pes_all_des_pos , file = paste0( catalog[ i , 'output_folder' ] , catalog[ i , 'all_design' ] )  , compress = FALSE )

			# final design object for people answering only short or long questionnaire #
			# # # # # # # # # # # # # # # # # 
			
			catalog[ i , 'case_count' ] <- nrow( pes_all_des )
			
			# delete the temporary files
			suppressWarnings( file.remove( tf , unzipped_files ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'output_folder' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}

