get_catalog_pof <-
	function( data_name = "pof" , output_dir , ... ){

		pof_ftp <- "ftp://ftp.ibge.gov.br/Orcamentos_Familiares/"
		
		ftp_listing <- readLines( textConnection( RCurl::getURL( pof_ftp ) ) )

		ay <- rev( gsub( "(.*) (.*)" , "\\2" , ftp_listing ) )
	
		# hardcoded removal of 1995-1996
		ay <- ay[ !( ay %in% c( "" , "Pesquisa_de_Orcamentos_Familiares_1995_1996" ) ) ]
	
		second_year <- gsub( "(.*)_([0-9]+)" , "\\2" , ay )
	
		catalog <-
			data.frame(
				full_urls = paste0( pof_ftp , ay , "/Microdados/Dados.zip" ) ,
				period = gsub( "Pesquisa_de_Orcamentos_Familiares_" , "" , ay ) ,
				documentation = paste0( pof_ftp , ay , "/Microdados/" , ifelse( second_year < 2009 , "Documentacao.zip" , "documentacao.zip" ) ) ,
				aliment_file = ifelse( second_year < 2009 , NA , paste0( pof_ftp , ay , "/Microdados/tradutores.zip" ) ) ,
				output_folder = paste0( output_dir , "/" , ay ) ,
				stringsAsFactors = FALSE
			)

		catalog

	}


lodown_pof <-
	function( data_name = "nppes" , catalog , path_to_7za = if( .Platform$OS.type != 'windows' ) '7za' else normalizePath( "C:/Program Files/7-zip/7z.exe" ) , ... ){
	
		if( system( paste0( '"' , path_to_7za , '" -h' ) , show.output.on.console = FALSE ) != 0 ) stop( paste0( "you need to install 7-zip.  if you already have it, include a parameter like path_to_7za='" , path_to_7za , "'" ) )
		
		if ( !requireNamespace( "gdata" , quietly = TRUE ) ) stop( "gdata needed for this function to work. to install it, type `install.packages( 'gdata' )`" , call. = FALSE )

		tf <- tempfile() ; tf2 <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			cachaca( catalog[ i , "full_urls" ] , tf , mode = 'wb' )

			unzipped_files <- unzip( tf , exdir = paste0( tempdir() , "/unzips" ) )
			
			cachaca( catalog[ i , "documentation" ] , tf , mode = 'wb' )

			doc_files <- unzip( tf , exdir = paste0( tempdir() , "/unzips" ) )

			files <- c( unzipped_files , doc_files )
			
			Encoding( files ) <- 'latin1'

			if( !is.na( catalog[ i , 'aliment_file' ] ) ){

				cachaca( catalog[ i , "aliment_file" ] , tf , mode = 'wb' )

				ali_files <- unzip( tf , exdir = paste0( tempdir() , "/unzips" ) )

				Encoding( ali_files ) <- 'latin1'
					
				# # # # # # # # # # # # # #
				# tables with food codes  #
				
				# figure out which is the alimentacao file
				cda <- ali_files[ grep( 'codigos_de_alimentacao' , tolower( ali_files ) ) ]
			
				# extract both tabs from the excel file
				componentes <- gdata::read.xls( cda , sheet = 1 , skip = 1 , colClasses = 'character' )
				estrutura <- gdata::read.xls( cda , sheet = 2 , skip = 1  , colClasses = 'character' )
						
				# reset the column names to be easily-readable
				names( componentes ) <- c( 'codigo' , 'nivel.1' , 'desc.1' , 'nivel.2' , 'desc.2' , 'nivel.3' , 'desc.3' )
				
				# the `estrutura` table should have the same column names,
				# except the first from `componentes`
				names( estrutura ) <- names( componentes )[ -1 ]
				
				
				# componentes table has a footnote, so throw it out
				# by removing all records with a missing
				# or empty `nivel.1` field
				componentes <- componentes[ !is.na( componentes$nivel.1 ) , ]
				componentes <- componentes[ componentes$nivel.1 != "" , ]
				
				
				# save both of these data frames to the local disk
				save( 
					componentes , estrutura , 
					file = paste0( catalog[ i , 'output_folder' ] , "/codigos de alimentacao.rda" ) 
				)
				
				# # # # # # # # # # # # # # # # #
				# table for post-stratification #
				
				# figure out which is the post-stratification table
				pos <- files[ grep( 'pos_estratos_totais' , tolower( files ) ) ]
			
				# extract the post-stratification table
				# from the excel file
				poststr <- gdata::read.xls( pos , sheet = 1 )
				# imported!  cool?  cool.
				
				# convert all column names to lowercase
				names( poststr ) <- tolower( names( poststr ) )
				
				# save this data frame to the local disk
				save( 
					poststr ,
					file = paste0( catalog[ i , 'output_folder' ] , "/poststr.rda" ) 
				)
				
				# remove all three of these tables from memory
				rm( componentes , estrutura , poststr )
				
			}
					




			# # # # # # # # # # # # # #
			# sas import organization #
			
			# before you worry about the data files,
			# get the sas import scripts under control.
			
			# extract the leitura file containing the sas importation instructions
			leitura <- files[ grep( 'leitura' , tolower( files ) ) ]

			# read the whole thing into memory
			z <- readLines( leitura )

			# remove all those goofy tab characters (which will screw up SAScii)
			z <- gsub( "\t" , " " , z )

			# remove lines containing the `if reg=__ then do;` pattern
			z <- z[ !grepl( 'if reg=.* then do;' , z ) ]
			
			# remove goofy @;
			z <- gsub( "@;" , "" , z )
			
			# remove lines containing solely `input`
			z <- z[ !( tolower( z ) == 'input' ) ]
			
			# remove the (SAScii-breaking) overlapping `controle` columns
			z <- z[ !grepl( "@3 controle 6." , z , fixed = TRUE ) ]
			
			# write the file back to your second temporary file
			writeLines( z , tf2 )

			# find each of your beginline parameters

			# find each line containing the string `INFILE` or `infile`
			all.beginlines <- grep( 'INFILE|infile' , z )
			
			# find line start positions
			start.pos <-
				unlist( 
					lapply(
						gregexpr( 
							"\\" , 
							z[ all.beginlines ] ,
							fixed = TRUE
						) ,
						max 
					) 
				) + 1
				
			# find line end positions
			end.pos <-
				unlist( 
						gregexpr( 
							".txt" , 
							z[ all.beginlines ] 
						) 
					) - 1 
				
			# isolate the names of all data files to be imported..
			data.files.to.import <-
				# pull the 14th character until `.txt` in the `INFILE` lines of the sas import script
				substr( 
					z[ all.beginlines ] , 
					start.pos , 
					end.pos
				)
			
			# now you've got an object containing the names of all data files that need to be imported.
			data.files.to.import
			
			# isolate the base filename before the period
			# for all downloaded files..
			all.file.basenames <-
				unlist( 
					lapply( 
						strsplit( 
							basename( files ) , 
							'.' , 
							fixed = TRUE 
						) , 
						'[[' , 
						1 
					) 
				)
			
			# for each data file name in `data.files.to.import`..
			for ( dfn in data.files.to.import ){

				# identify which .7z file contains the data	
				if ( tolower( dfn ) == 't_rendimentos' ) {
					data.file <- files[ which( 't_rendimentos1' == tolower( all.file.basenames ) ) ] 
				} else {
					data.file <- files[ which( tolower( dfn ) == tolower( all.file.basenames ) ) ]
				}
			
			
				# if `data.file` contains multiple files..
				if ( length( data.file ) > 1 ){
				
					# pick the zipped file..
					data.file <- data.file[ grep( '.zip' , tolower( data.file ) , fixed = TRUE ) ]
					
					# ..unzip it, and overwrite `data.file` with the new filepath
					data.file <- unzip( data.file , exdir = tempdir() )
				}
				
			
				# and now, if the data.file is just a text file..
				if ( grepl( "txt$" , tolower( data.file ) ) ){

					# then no unzipping is necessary
					curfile <- data.file
					
				# otherwise, the file must be unzipped with 7-zip
				} else {
				
					# build the string to send to DOS
					dos.command <- paste0( '"' , path_to_7za , '" x ' , data.file )

					# extract the file, platform-specific
					if ( .Platform$OS.type != 'windows' ) system( dos.command ) else shell( dos.command )

					# find the name of the final ASCII data file to be imported
					curfile <- gsub( ".7z" , ".txt" , basename( data.file ) )

				}
				
				# figure out which beginline position to use
				cur.beginline <- which( tolower( dfn ) == tolower( data.files.to.import ) )
				
				# import the data file into R
				x <- 
					read_SAScii( 
						curfile , 
						tf2 , 
						beginline = all.beginlines[ cur.beginline ] ,
						skip_decimal_division = TRUE
					)
				
				# convert all column names to lowercase
				names( x ) <- tolower( names( x ) )
				
				# rename the data table appropriately
				assign( tolower( dfn ) , x )
				
				# save the current data.frame
				# to the appropriate year folder
				# within the current working directory
				save( 
					list = tolower( dfn ) , 
					file = paste0( catalog[ i , 'output_folder' ] , "/" , tolower( dfn ) , ".rda" )
				)

				# delete the current file from the current working directory
				file.remove( curfile )
						
			}
				
			# revert the encoding for more effective deletion.
			Encoding( files ) <- Encoding( ali_files ) <- ''
		

						
			# delete the temporary files
			file.remove( tf , files , ali_files )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored at '" , catalog[ i , 'output_filename' ] , "'\r\n\n" ) )

		}

		invisible( TRUE )

	}

