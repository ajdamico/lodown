get_catalog_pnadc <-
	function( data_name = "pnadc" , output_dir , ... ){

		# initiate the full ftp path
		year.ftp <- "ftp://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/Trimestral/Microdados/"

		# read the text of the microdata ftp into working memory
		# download the contents of the ftp directory for all microdata
		year.listing <- readLines( textConnection( RCurl::getURL( year.ftp ) ) )

		# extract all years
		year.lines <- gsub( "(.*)([0-9][0-9][0-9][0-9])(</A>)?" , "\\2" , year.listing )
		
		suppressWarnings( year.lines <- year.lines[ !is.na( as.numeric( year.lines ) ) ] )

		# initiate an empty vector
		zip.filenames <- NULL

		# loop through every year
		for ( this.year in year.lines ){

			# find the zipped files in the year-specific folder
			ftp.listing <- readLines( textConnection( RCurl::getURL( paste0( year.ftp , this.year , "/" ) ) ) )

			# break up the string based on the ending extension
			zip.lines <- grep( "\\.zip$" , ftp.listing , value = TRUE )

			# extract the precise filename of the `.zip` file,
			# and add it to the zip filenames vector.
			zip.filenames <- c( zip.filenames , gsub( '(.*) (.*)' , "\\2" , zip.lines ) )
		}

		quarter <- gsub( "(.*)PNADC_([0-9][0-9])([0-9][0-9][0-9][0-9])(.*)\\.(zip|ZIP)" , "\\2" , zip.filenames )
		
		year <- gsub( "(.*)PNADC_([0-9][0-9])([0-9][0-9][0-9][0-9])(.*)\\.(zip|ZIP)" , "\\3" , zip.filenames )

		catalog <-
			data.frame(
				year = gsub( "(.*)PNADC_([0-9][0-9])([0-9][0-9][0-9][0-9])(.*)\\.(zip|ZIP)" , "\\3" , zip.filenames ) ,
				quarter = gsub( "(.*)PNADC_([0-9][0-9])([0-9][0-9][0-9][0-9])(.*)\\.(zip|ZIP)" , "\\2" , zip.filenames ) ,
				stringsAsFactors = FALSE
			)

		catalog$full_url <- paste0( year.ftp , catalog$year , "/" , zip.filenames )
		
		catalog$output_filename <- paste0( output_dir , '/pnadc ' , catalog$year , ' ' , catalog$quarter , '.rds' )
		
		catalog

	}


lodown_pnadc <-
	function( data_name = "pnadc" , catalog , ... ){
		
		on.exit( print( catalog ) )

		tf <- tempfile()

		# initiate the full ftp path
		doc_ftp <- "ftp://ftp.ibge.gov.br/Trabalho_e_Rendimento/Pesquisa_Nacional_por_Amostra_de_Domicilios_continua/Trimestral/Microdados/Documentacao/"

		doc_path <- paste0( doc_ftp , gsub( "(.*) (.*)" , "\\2" , grep( "Dicionario_e_input" , readLines( textConnection( RCurl::getURL( doc_ftp ) ) ) , value = TRUE ) ) )
		
		cachaca( doc_path , tf , mode = 'wb' , attempts = 10 )

		sasfile <- grep( "\\.sas$" , unzip_warn_fail( tf , exdir = tempdir() ) , value = TRUE , ignore.case = TRUE )
			
		if( length( sasfile ) != 1 ) stop( 'only expecting one sas file within the documentation' )

		for ( i in seq_len( nrow( catalog ) ) ){

			# download the file
			cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' , attempts = 10 )

			unzipped_files <- unzip_warn_fail( tf , exdir = paste0( tempdir() , "/unzips" ) )

			txt_file <- grep( "\\.txt$" , unzipped_files , value = TRUE )

			if( length( txt_file ) != 1 ) stop( 'only expecting one txt file within each zip' )
				
			# ..and read that text file directly into an R data.frame
			# using the sas importation script downloaded before this big fat loop
			x <- read_SAScii( txt_file , sasfile )

			# immediately make every field numeric
			for( j in names( x ) ) x[ , j ] <- as.numeric( as.character( x[ , j ] ) )

			# convert all column names to lowercase
			names( x ) <- tolower( names( x ) )

			saveRDS( x , file = catalog[ i , 'output_filename' ] , compress = FALSE )

			catalog[ i , 'case_count' ] <- nrow( x )
			
			# delete the temporary files
			file.remove( tf , unzipped_files )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		on.exit()
		
		catalog

	}

