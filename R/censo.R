get_catalog_censo <-
	function( data_name = "censo" , output_dir , ... ){

		catalog <- NULL

		# designate the location of the 2010 general sample microdata files
		ftp_path_2010 <-	"ftp://ftp.ibge.gov.br/Censos/Censo_Demografico_2010/Resultados_Gerais_da_Amostra/Microdados/"

		# fetch all available files in the ftp site's directory
		all_files <- RCurl::getURL( ftp_path_2010 , dirlistonly = TRUE )

		# those files are separated by newline characters in the code,
		# so simply split them up into a character vector
		# full of individual zipped file strings
		all_files <- scan( text = all_files , what = "character", quiet = T )

		# remove the two files you don't need to import
		files_to_download_2010 <- all_files[ !( all_files %in% c( '1_Atualizacoes_20160311.txt' , 'Atualizacoes.txt' , 'Documentacao.zip' ) ) ]

		catalog <-
			rbind( 
				catalog ,
				data.frame(
					full_url = paste0( ftp_path_2010 , files_to_download_2010 ) ,
					year = 2010 ,
					db_table_prefix = tolower( gsub( ".zip" , "10" , files_to_download_2010 , ignore.case = TRUE ) ) ,
					dbfolder = paste0( output_dir , "/MonetDB" ) ,
					pes_design = paste0( output_dir , "/pes 2010 design.rds" ) ,
					pes_sas = system.file("extdata", "censo/SASinputPes.txt", package = "lodown") ,
					dom_design = paste0( output_dir , "/dom 2010 design.rds" ) ,
					dom_sas = system.file("extdata", "censo/SASinputDom.txt", package = "lodown") ,
					fam_design = NA ,
					fam_sas = NA ,
					dom_ranc = NA ,
					pes_ranc = NA ,
					fam_ranc = NA ,
					fpc1 = 'v0011' ,
					fpc2 = 'v0010' ,
					fpc3 = 'v0001' ,
					fpc4 = 'v0300' ,
					fpc5 = NA ,
					weight = 'v0010' ,
					stringsAsFactors = FALSE
				)
			)

			
		# designate the location of the 2000 general sample microdata files
		ftp_path_2000 <-	"ftp://ftp.ibge.gov.br/Censos/Censo_Demografico_2000/Microdados/"

		# fetch all available files in the ftp site's directory
		all_files <- RCurl::getURL( ftp_path_2000 , dirlistonly = TRUE )

		# those files are separated by newline characters in the code,
		# so simply split them up into a character vector
		# full of individual zipped file strings
		all_files <- scan( text = all_files , what = "character", quiet = T )

		# remove the two files you don't need to import
		files_to_download_2000 <- all_files[ !( all_files %in% c( '1_Documentacao.zip' , '2_Atualizacoes.txt' , '1_Documentacao_velho.zip' , "2_Atualizacoes_20160309.txt" , "1_Documentacao_20160309.zip" ) ) ]

		catalog <-
			rbind( 
				catalog ,
				data.frame(
					full_url = paste0( ftp_path_2000 , files_to_download_2000 ) ,
					year = 2000 ,
					db_table_prefix = tolower( gsub( ".zip" , "00" , files_to_download_2000 , ignore.case = TRUE ) ) ,
					dbfolder = paste0( output_dir , "/MonetDB" ) ,
					pes_design = paste0( output_dir , "/pes 2000 design.rds" ) ,
					pes_sas = system.file("extdata", "censo/LE_PESSOAS.sas", package = "lodown") ,
					dom_design = paste0( output_dir , "/dom 2000 design.rds" ) ,
					dom_sas = system.file("extdata", "censo/LE_DOMIC.sas", package = "lodown") ,
					fam_design = paste0( output_dir , "/fam 2000 design.rds" ) ,
					fam_sas = system.file("extdata", "censo/LE_FAMILIAS.sas", package = "lodown") ,
					dom_ranc = 170 ,
					pes_ranc = 390 ,
					fam_ranc = 118 ,
					fpc1 = "areap" ,
					fpc2 = "p001" ,
					fpc3 = 'v0102' ,
					fpc4 = 'v0300' ,
					fpc5 = 'v0404' ,
					weight = 'p001' ,
					stringsAsFactors = FALSE
				)
			)
		
		catalog
	
	}

lodown_censo <-
	function( data_name = "censo" , catalog , ... ){

		tf <- tempfile()

		for ( i in seq_len( nrow( catalog ) ) ){

			# open the connection to the monetdblite database
			db <- DBI::dbConnect( MonetDBLite::MonetDBLite() , catalog[ i , 'dbfolder' ] )

			cachaca( catalog[ i , 'full_url' ] , tf , mode = 'wb' )
			
			unzipped_files <- unzip_warn_fail( tf , exdir = tempdir() )

			dom_file <- unzipped_files[ grep( 'DOM' , unzipped_files , useBytes = TRUE , ignore.case = TRUE ) ]
			pes_file <- unzipped_files[ grep( 'PES' , unzipped_files , useBytes = TRUE , ignore.case = TRUE ) ]
			fam_file <- unzipped_files[ grep( 'FAM' , toupper( unzipped_files ) , useBytes = TRUE , ignore.case = TRUE ) ]

			if( !is.na( catalog[ i , 'dom_ranc' ] ) ) dom_file <- ranc_censo( dom_file , catalog[ i , 'dom_ranc' ] )
			if( !is.na( catalog[ i , 'pes_ranc' ] ) ) pes_file <- ranc_censo( pes_file , catalog[ i , 'pes_ranc' ] )
			if( !is.na( catalog[ i , 'fam_ranc' ] ) ) fam_file <- ranc_censo( fam_file , catalog[ i , 'fam_ranc' ] )
			
			
			for( this_dom in dom_file ){
			
				read_SAScii_monetdb (
					this_dom ,
					sas_ri = catalog[ i , 'dom_sas' ] ,
					zipped = FALSE ,
					tl = TRUE ,
					tablename = paste0( catalog[ i , 'db_table_prefix' ] , '_dom' ) ,
					connection = db
				)

			}
			
			
			for( this_pes in pes_file ){
			
				read_SAScii_monetdb (
					this_pes ,
					sas_ri = catalog[ i , 'pes_sas' ] ,
					zipped = FALSE ,
					tl = TRUE ,
					tablename = paste0( catalog[ i , 'db_table_prefix' ] , '_pes' ) ,
					connection = db
				)

			}
			
			for( this_fam in fam_file ){
			
				read_SAScii_monetdb (
					this_fam ,
					sas_ri = catalog[ i , 'fam_sas' ] ,
					zipped = FALSE ,
					tl = TRUE ,
					tablename = paste0( catalog[ i , 'db_table_prefix' ] , '_fam' ) ,
					connection = db
				)

			}
			
			# add the number of records to the catalog
			catalog[ i , 'case_count' ] <- DBI::dbGetQuery( db , paste0( "SELECT COUNT(*) FROM " , catalog[ i , 'db_table_prefix' ] , '_pes' ) )[ 1 , 1 ]

			# disconnect from the current monet database
			DBI::dbDisconnect( db , shutdown = TRUE )

			cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'dbfolder' ] , "'\r\n\n" ) )

		}
		
		# create unique survey designs
		dom_designs <- unique( catalog[ , c( "year" , "dbfolder" , "dom_design" , 'weight' , paste0( 'fpc' , 1:5 ) ) ] )
		pes_designs <- unique( catalog[ , c( "year" , "dbfolder" , "pes_design" , 'weight' , paste0( 'fpc' , 1:5 ) ) ] )
		fam_designs <- unique( catalog[ , c( "year" , "dbfolder" , "fam_design" , 'weight' , paste0( 'fpc' , 1:5 ) ) ] )
		
		names( dom_designs ) <- c( "year" , 'dbfolder' , 'design' , 'weight' , paste0( 'fpc' , 1:5 ) )
		names( pes_designs ) <- c( "year" , 'dbfolder' , 'design' , 'weight' , paste0( 'fpc' , 1:5 ) )
		names( fam_designs ) <- c( "year" , 'dbfolder' , 'design' , 'weight' , paste0( 'fpc' , 1:5 ) )
		
		dom_designs$type <- 'dom'
		pes_designs$type <- 'pes'
		fam_designs$type <- 'fam'
		
		unique_designs <- rbind( dom_designs , pes_designs , fam_designs )
		
		unique_designs <- unique_designs[ !is.na( unique_designs$design ) , ]
		
		# dom first, fam second, pes third
		unique_designs <- unique_designs[ order( unique_designs$type ) , ]
		
		for( i in seq_len( nrow( unique_designs ) ) ){

			# open the connection to the monetdblite database
			db <- DBI::dbConnect( MonetDBLite::MonetDBLite() , unique_designs[ i , 'dbfolder' ] )

			these_tables <- 
				paste0( 
					catalog[ catalog$dbfolder %in% unique_designs[ i , 'dbfolder' ] & catalog[ , paste0( unique_designs[ i , 'type' ] , "_design" ) ] %in% unique_designs[ i , 'design' ] , 'db_table_prefix' ] , 
					"_" , 
					unique_designs[ i , 'type' ] 
				)
			
			this_stack <-
				paste0(
					'create table c' , 
					substr( unique_designs[ i , 'year' ] , 3 , 4 ) , 
					"_" , 
					unique_designs[ i , 'type' ] , 
					'_pre_fpc as (SELECT * FROM ' ,
					paste0( these_tables , collapse = ') UNION ALL (SELECT * FROM ' ) ,
					') WITH DATA'
				)

			DBI::dbSendQuery( db , this_stack )

			this_create <-
				paste0( 'create table c' , 
					substr( unique_designs[ i , 'year' ] , 3 , 4 ) , 
					'_' , 
					unique_designs[ i , 'type' ] , 
					'_fpc as (select ' , 
					unique_designs[ i , 'fpc1' ] ,
					' , sum( ' ,
					unique_designs[ i , 'fpc2' ] ,
					' ) as sum_fpc2 from c' , 
					substr( unique_designs[ i , 'year' ] , 3 , 4 ) , 
					'_' , 
					unique_designs[ i , 'type' ] , 
					'_pre_fpc group by ' , 
					unique_designs[ i , 'fpc1' ] ,
					') WITH DATA' )

			DBI::dbSendQuery( db , this_create )

			if( unique_designs[ i , 'type' ] == 'dom' ){
			
				count_create <-
					paste0(
						'create table c' ,
						substr( unique_designs[ i , 'year' ] , 3 , 4 ) , 
						'_dom_count_pes as (select ' ,
						unique_designs[ i , 'fpc3' ] , 
						' , ' ,
						unique_designs[ i , 'fpc4' ] ,
						' , count(*) as dom_count_pes from c' ,
						substr( unique_designs[ i , 'year' ] , 3 , 4 ) ,
						'_' ,
						unique_designs[ i , 'type' ] ,
						'_pre_fpc group by ' ,
						unique_designs[ i , 'fpc3' ] , 
						' , ' ,
						unique_designs[ i , 'fpc4' ] ,
						' ) WITH DATA' )

				DBI::dbSendQuery( db , count_create )
				
				dom_fpc_merge <-
					paste0( 
						'create table c' ,
						substr( unique_designs[ i , 'year' ] , 3 , 4 ) , 
						'_dom as ( select a1.* , b1.dom_count_pes from (select a2.* , b2.sum_fpc2 as dom_fpc from c' ,
						substr( unique_designs[ i , 'year' ] , 3 , 4 ) , 
						'_dom_pre_fpc as a2 inner join c' , 
						substr( unique_designs[ i , 'year' ] , 3 , 4 ) , 
						'_dom_fpc as b2 on a2.' ,
						unique_designs[ i , 'fpc1' ] ,
						' = b2.' ,
						unique_designs[ i , 'fpc1' ] ,
						') as a1 inner join c' ,
						substr( unique_designs[ i , 'year' ] , 3 , 4 ) , 
						'_dom_count_pes as b1 on a1.' ,
						unique_designs[ i , 'fpc3' ] ,
						' = b1.' ,
						unique_designs[ i , 'fpc3' ] ,
						' AND a1.' ,
						unique_designs[ i , 'fpc4' ] ,
						' = b1.' ,
						unique_designs[ i , 'fpc4' ] ,
						' ) WITH DATA'
					)
					
				DBI::dbSendQuery( db , dom_fpc_merge )

			} else {
				
				final_merge <-
					paste0( 
						'create table c' , 
						substr( unique_designs[ i , 'year' ] , 3 , 4 ) , 
						'_' , 
						unique_designs[ i , 'type' ] , 
						' as (select a.* , b.sum_fpc2 as ' ,
						unique_designs[ i , 'type' ] ,
						'_fpc from c' , 
						substr( unique_designs[ i , 'year' ] , 3 , 4 ) , 
						'_' ,
						unique_designs[ i , 'type' ] ,
						'_pre_fpc as a inner join c' , 
						substr( unique_designs[ i , 'year' ] , 3 , 4 ) , 
						'_' ,
						unique_designs[ i , 'type' ] ,
						'_fpc as b on a.' ,
						unique_designs[ i , 'fpc1' ] ,
						' = b.' ,
						unique_designs[ i , 'fpc1' ] ,
						') WITH DATA'
					)
				
				DBI::dbSendQuery( db , final_merge )
			
			}
		
		
			DBI::dbSendQuery( 
				db , 
				paste0( 
					"ALTER TABLE c" , 
					substr( unique_designs[ i , 'year' ] , 3 , 4 ) ,
					"_" ,
					unique_designs[ i , 'type' ] , 
					' ADD COLUMN ' ,
					unique_designs[ i , 'type' ] , 
					'_wgt DOUBLE PRECISION'
				)
			)
		
			DBI::dbSendQuery( 
				db , 
				paste0( 
					"UPDATE c" , 
					substr( unique_designs[ i , 'year' ] , 3 , 4 ) ,
					"_" ,
					unique_designs[ i , 'type' ] , 
					' SET ' ,
					unique_designs[ i , 'type' ] , 
					'_wgt = ' ,
					unique_designs[ i , 'weight' ]
				)
			)

			DBI::dbSendQuery( 
				db , 
				paste0( 
					"ALTER TABLE c" , 
					substr( unique_designs[ i , 'year' ] , 3 , 4 ) ,
					"_" ,
					unique_designs[ i , 'type' ] , 
					' DROP COLUMN ' ,
					unique_designs[ i , 'weight' ]
				)
			)
			
			if( unique_designs[ i , 'type' ] == 'fam' ){
				
				b_fields <- DBI::dbListFields( db , paste0( "c" , substr( unique_designs[ i , 'year' ] , 3 , 4 ) , '_fam' ) )[ !( DBI::dbListFields( db , paste0( "c" , substr( unique_designs[ i , 'year' ] , 3 , 4 ) , '_fam' ) ) %in% DBI::dbListFields( db , paste0( "c" , substr( unique_designs[ i , 'year' ] , 3 , 4 ) , '_dom' ) ) ) ]

				semifinal_merge <-
					paste0(
						'create table c' , substr( unique_designs[ i , 'year' ] , 3 , 4 ) , '_dom_fam as (SELECT a.* , b.' ,
						paste( b_fields , collapse = ', b.' ) ,
						' from c' , substr( unique_designs[ i , 'year' ] , 3 , 4 ) , '_dom as a inner join c' , substr( unique_designs[ i , 'year' ] , 3 , 4 ) , '_fam as b ON a.' , unique_designs[ i , 'fpc3' ] , ' = b.' , unique_designs[ i , 'fpc3' ] , ' AND a.' , unique_designs[ i , 'fpc4' ] , ' = b.' , unique_designs[ i , 'fpc4' ] , ') WITH DATA'
					)
					
				DBI::dbSendQuery( db , semifinal_merge )
			
			}
			
			if( unique_designs[ i , 'type' ] == 'pes' ){
				
				if( paste0( 'c' , substr( unique_designs[ i , 'year' ] , 3 , 4 ) , '_fam' ) %in% DBI::dbListTables( db ) ){
					
					b_fields <- DBI::dbListFields( db , paste0( 'c' , substr( unique_designs[ i , 'year' ] , 3 , 4 ) , '_pes' ) )[ !( DBI::dbListFields( db , paste0( 'c' , substr( unique_designs[ i , 'year' ] , 3 , 4 ) , '_pes' ) ) %in% DBI::dbListFields( db , paste0( 'c' , substr( unique_designs[ i , 'year' ] , 3 , 4 ) , '_dom_fam' ) ) ) ]

					final_merge <-
						paste0(
							'create table c' , substr( unique_designs[ i , 'year' ] , 3 , 4 ) , ' as (SELECT a.* , b.' ,
							paste( b_fields , collapse = ', b.' ) ,
							' from c' , substr( unique_designs[ i , 'year' ] , 3 , 4 ) , '_dom_fam as a inner join c' , substr( unique_designs[ i , 'year' ] , 3 , 4 ) , '_pes as b ON a.' , unique_designs[ i , 'fpc3' ] , ' = b.' , unique_designs[ i , 'fpc3' ] , ' AND a.' , unique_designs[ i , 'fpc4' ] , ' = b.' , unique_designs[ i , 'fpc4' ] , ' AND a.' , unique_designs[ i , 'fpc5' ] , ' = b.' , unique_designs[ i , 'fpc5' ] , ' ) WITH DATA'
						)
				
				} else {
				

					b_fields <- DBI::dbListFields( db , paste0( 'c' , substr( unique_designs[ i , 'year' ] , 3 , 4 ) , '_pes' ) )[ !( DBI::dbListFields( db , paste0( 'c' , substr( unique_designs[ i , 'year' ] , 3 , 4 ) , '_pes' ) ) %in% DBI::dbListFields( db , paste0( 'c' , substr( unique_designs[ i , 'year' ] , 3 , 4 ) , '_dom' ) ) ) ]

					final_merge <-
						paste0(
							'create table c' , substr( unique_designs[ i , 'year' ] , 3 , 4 ) , ' as (SELECT a.* , b.' ,
							paste( b_fields , collapse = ', b.' ) ,
							' from c' , substr( unique_designs[ i , 'year' ] , 3 , 4 ) , '_dom as a inner join c' , substr( unique_designs[ i , 'year' ] , 3 , 4 ) , '_pes as b ON a.' , unique_designs[ i , 'fpc3' ] , ' = b.' , unique_designs[ i , 'fpc3' ] , ' AND a.' , unique_designs[ i , 'fpc4' ] , ' = b.' , unique_designs[ i , 'fpc4' ] , ' ) WITH DATA'
						)


				
				}

						
				DBI::dbSendQuery( db , final_merge )
			
			}
			
		
			# add columns named 'one' to each table..
			DBI::dbSendQuery( 
				db , 
				paste0( 
					'alter table c' , 
					substr( unique_designs[ i , 'year' ] , 3 , 4 ) , 
					'_' , 
					unique_designs[ i , 'type' ] , 
					' add column one int' 
				)
			)
			
			
			# ..and fill them all with the number 1.
			DBI::dbSendQuery( 
				db , 
				paste0(
					'UPDATE c' , 
					substr( unique_designs[ i , 'year' ] , 3 , 4 ) , 
					'_' , 
					unique_designs[ i , 'type' ] , 
					' SET one = 1' 
				)
			)
			
			if( unique_designs[ i , 'type' ] == 'pes' ){
				
				stopifnot( 
					DBI::dbGetQuery( db , paste0( "select count(*) as count from c" , substr( unique_designs[ i , 'year' ] , 3 , 4 ) , "_pes" ) ) == 
					DBI::dbGetQuery( db , paste0( "select count(*) as count from c" , substr( unique_designs[ i , 'year' ] , 3 , 4 ) ) )
				)
			
			}
			
			bootw <- 
				survey::bootweights( 
					DBI::dbGetQuery( db , paste0( "SELECT " , unique_designs[ i , 'fpc1' ] , " FROM c" , substr( unique_designs[ i , 'year' ] , 3 , 4 ) , ifelse( unique_designs[ i , 'type' ] == 'pes' , "" , paste0( "_" , unique_designs[ i , 'type' ] ) ) ) )[ , 1 ] ,
					DBI::dbGetQuery( db , paste0( "SELECT " , unique_designs[ i , 'fpc4' ] , " FROM c" , substr( unique_designs[ i , 'year' ] , 3 , 4 ) , ifelse( unique_designs[ i , 'type' ] == 'pes' , "" , paste0( "_" , unique_designs[ i , 'type' ] ) ) ) )[ , 1 ] ,
					replicates = 80 ,
					fpc = DBI::dbGetQuery( db , paste0( "SELECT " , unique_designs[ i , 'type' ] , "_fpc FROM c" , substr( unique_designs[ i , 'year' ] , 3 , 4 ) , ifelse( unique_designs[ i , 'type' ] == 'pes' , "" , paste0( "_" , unique_designs[ i , 'type' ] ) ) ) )[ , 1 ]
				)

			this_design <-
				survey::svrepdesign(
					weight = as.formula( paste0( "~" , unique_designs[ i , 'type' ] , "_wgt" ) ) ,
					repweights = bootw$repweights ,
					combined.weights = FALSE ,
					scale = bootw$scale ,
					rscales = bootw$rscales ,
					data = paste0( 'c' , substr( unique_designs[ i , 'year' ] , 3 , 4 ) , ifelse( unique_designs[ i , 'type' ] == 'pes' , "" , paste0( "_" , unique_designs[ i , 'type' ] ) ) ) ,
					dbtype = "MonetDBLite" ,
					dbname = unique_designs[ i , 'dbfolder' ]
				)

			saveRDS( this_design , file = unique_designs[ i , 'design' ] )
			
			# disconnect from the current monet database
			DBI::dbDisconnect( db , shutdown = TRUE )

			cat( paste0( data_name , " survey design entry " , i , " of " , nrow( unique_designs ) , " stored at '" , unique_designs[ i , 'design' ] , "'\r\n\n" ) )
			
		}
		
		catalog

	}

	

# define a special function to   #
# remove alphanumeric characters #
# from any data files that have  #
# been downloaded from ibge      #
ranc_censo <- 
	function( infiles , width , encoding = getOption("encoding") ){

		tf_a <- tempfile()

		outcon <- file( tf_a , "w" )

		# if there are multiple infiles,
		# loop through them all!
		for ( infile in infiles ){

			incon <- file( infile , "r" , encoding = encoding )

			line.num <- 0
			
			while( length( line <- readLines( incon , 1 , skipNul = TRUE , warn = FALSE ) ) > 0 ){

				# add blank spaces on the right side where they're absent.
				line <- stringr::str_pad( line , width , side = "right" , pad = " " )
				
				# save the file on the disk, so long as there's no weird corruption
				if( !is.na( iconv( line , "" , "ASCII" ) ) ) writeLines( line , outcon )
				# like line 509,451 and 2,575,789 of the combined 2000 sao paulo file.

				# add to the line counter #
				line.num <- line.num + 1

			}

			close( incon )
		}

		close( outcon )

		tf_a
	}
