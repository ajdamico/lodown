get_catalog_censo_escolar <-
	function( data_name = "censo_escolar" , output_dir , ... ){

		inep_portal <- "http://portal.inep.gov.br/basica-levantamentos-acessar"

		portal_table <- rvest::html_table( xml2::read_html( inep_portal ) , fill = TRUE )[[1]]

		year_lines <- portal_table[ portal_table$X1 == "Microdados Censo Escolar" , 'X2' ]
		
		year_lines <- iconv( year_lines , "" , "ASCII" , sub = " " )

		year_lines <- gsub( " +" , " " , year_lines )

		censo_escolar_years <- strsplit( year_lines , " " )[[1]]
	
		catalog <-
			data.frame(
				year = censo_escolar_years ,
				full_url = paste0( "http://download.inep.gov.br/microdados/micro_censo_escolar" , censo_escolar_years , ".zip" ) ,
				db_tablename = paste0( "x" , censo_escolar_years ) ,
				dbfolder = paste0( output_dir , "/MonetDB" ) ,
				output_folder = paste0( output_dir , "/" , censo_escolar_years ) ,
				stringsAsFactors = FALSE
			)

		catalog

	}


lodown_censo_escolar <-
	function( data_name = "censo_escolar" , catalog , ... ){

		tf <- tempfile()


		for ( i in seq_len( nrow( catalog ) ) ){

			# open the connection to the monetdblite database
			db <- DBI::dbConnect( MonetDBLite::MonetDBLite() , catalog[ i , 'dbfolder' ] )

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

			unzipped_files <- unzip( tf , exdir = paste0( tempdir() , "/unzips" ) )

			# now the files are unzipped.. read them into the monetdb table using tablename
			
				# catalog[ i , 'db_tablename' ]


			# also save the documentation in the
				# catalog[ i , 'output_folder' ]


			# delete the temporary files?  or move some docs to a save folder?
			# suppressWarnings( file.remove( tf , unzipped_files ) )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'dbfolder' ] , "'\r\n\n" ) )

		}

		invisible( TRUE )

	}

