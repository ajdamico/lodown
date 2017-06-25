get_catalog_censo_escolar <-
  function( data_name = "censo_escolar" , output_dir , ... ){

    inep_portal <- "http://portal.inep.gov.br/microdados"

    w <- rvest::html_attr( rvest::html_nodes( xml2::read_html( inep_portal ) , "a" ) , "href" )

    these_links <- grep( "censo_escolar(.*)zip$" , w , value = TRUE , ignore.case = TRUE )

    censo_escolar_years <- substr( gsub( "[^0-9]" , "" , these_links ) , 1 , 4 )

    catalog <-
      data.frame(
        year = censo_escolar_years ,
        dbfolder = paste0( output_dir , "/MonetDB" ) ,
        output_folder = paste0( output_dir , "/" , censo_escolar_years ) ,
        full_url = these_links ,
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

      unzipped_files <- unzip_warn_fail( tf , exdir = catalog[ i , "output_folder" ] )

      for ( these_zips in grep( "\\.zip$" , unzipped_files , value = TRUE , ignore.case = TRUE ) ) unzipped_files <- c( unzipped_files , unzip_warn_fail( these_zips , exdir = np_dirname( these_zips ) ) )

	  unzipped_files <- gsub( "\\\\" , if( .Platform$OS.type != 'windows' ) "_" else "/" , unzipped_files )
	  
      if( catalog[ i , 'year' ] <= 2006 ){

        sas_files <- grep( "\\.sas$", unzipped_files, value = TRUE , ignore.case = TRUE )

        sas_scaledowns <- gsub( "SAS|_" , "" , gsub( "\\.sas|\\.SAS" , "" , gsub( paste0( "INPUT|" , catalog[ i , 'year' ] ) , "" , basename( sas_files ) ) ) )
        sas_scaledowns <- ifelse( grepl( "ESC" , sas_scaledowns ) & !grepl( "INDICESC" , sas_scaledowns ) , gsub( "ESC" , "" , sas_scaledowns ) , sas_scaledowns )

        datafile_matches <- lapply( sas_scaledowns , function( z ) unzipped_files[ grepl( z , basename( unzipped_files ) , ignore.case = TRUE ) & grepl( "dados" , unzipped_files , ignore.case = TRUE ) ] )
        datafile_matches <- lapply( datafile_matches , function( z ) z[ !grepl( "\\.zip" , z , ignore.case = TRUE ) ] )

        these_tables <-
          data.frame(
            sas_script = sas_files ,
            data_file = unique( unlist( datafile_matches ) ) ,
            db_tablename = paste0( tolower( sas_scaledowns ) , "_" , catalog[ i , 'year' ] ) ,
            stringsAsFactors = FALSE
          )

        for ( j in seq( nrow( these_tables ) ) ){

          Encoding( these_tables[ j , 'sas_script' ] ) <- ''

          # write the file to the disk
          w <- readLines( these_tables[ j , 'sas_script' ] )

          # remove all tab characters
          w <- gsub( '\t' , ' ' , w )

          w <- gsub( '@ ' , '@' , w , fixed = TRUE )
          w <- gsub( "@371 CEST_SAUDE  " , "@371 CEST_SAUDE $1 " , w , fixed = TRUE )
          w <- gsub( "@379 OUTROS  " , "@379 OUTROS $1 " , w , fixed = TRUE )
          w <- gsub( "VEF918 11" , "VEF918 11" , w , fixed = TRUE )
          w <- gsub( "VEE1411( +)/" , "VEE1411 7. /" , w )
          w <- gsub( "VEE1412( +)/" , "VEE1412 7. /" , w )

          # overwrite the file on the disk with the newly de-tabbed text
          writeLines( w , these_tables[ j , 'sas_script' ] )

          x <- read_SAScii( these_tables[ j , 'data_file' ] , these_tables[ j , 'sas_script' ] , na_values = c( "" , "." ) )

          # convert column names to lowercase
          names( x ) <- tolower( names( x ) )

          # do not use monetdb reserved words
          for ( k in names( x )[ toupper( names( x ) ) %in% getFromNamespace( "reserved_monetdb_keywords" , "MonetDBLite" ) ] ) names( x )[ names( x ) == k ] <- paste0( k , "_" )

          DBI::dbWriteTable( db , these_tables[ j , 'db_tablename' ] , x )

          rm( x )

          cat( tolower( gsub( "\\..*" , "" , basename( these_tables[ j , 'data_file' ] ) ) ) , "stored at" ,
               these_tables[ j , 'db_tablename' ] , "\r\n\n" )

          catalog[ i , 'case_count' ] <- max( catalog[ i , 'case_count' ] , DBI::dbGetQuery( db , paste( "SELECT COUNT(*) FROM" , these_tables[ j , 'db_tablename' ] ) )[ 1 , 1 ] , na.rm = TRUE )

        }

      } else {

        this_excel_file <- unzipped_files [ grep( "^ANEXO I -" , basename( unzipped_files ), value = FALSE, ignore.case = TRUE ) ]
        data_files <- grep( "\\.rar$|\\.csv$", unzipped_files, value = TRUE , ignore.case = TRUE )

        for ( this_table_type in c( "docente" , "matricula" , "turma" , "escola" ) ) {

          tabelas <- data_files[ grep( this_table_type , basename( data_files ), value = FALSE, ignore.case = TRUE ) ]

          for ( j in seq_along( tabelas ) ) {

            codebook <- read_excel_metadata( this_excel_file , this_table_type )

            if ( grepl( "\\.rar$" , tabelas[ j ] , ignore.case = TRUE ) ) {

				archive::archive_extract( normalizePath( tabelas[ j ] ) , dir = normalizePath( catalog[ i , "output_folder" ] ) )

				this_data_file <- list.files( catalog[ i , "output_folder" ] , full.names = TRUE )

				this_data_file <- grep( "\\.csv$", this_data_file, value = TRUE, ignore.case = TRUE )

            } else {

              this_data_file <- tabelas[ j ]

            }

            columns.in.datafile <- strsplit( readLines( this_data_file , n = 1 ) , "\\|" )[ 1 ] [[ 1 ]]
            columns.in.datafile <- tolower( columns.in.datafile )
            codebook <- codebook[ codebook$`Nome da Variavel` %in% columns.in.datafile , ]
            codebook <- codebook[ match( columns.in.datafile , codebook$`Nome da Variavel` ) , ]

            if ( this_table_type == "matricula" ) {

              suppressMessages(
                DBI::dbWriteTable(
                  db,
                  paste0( this_table_type , "_" , catalog[ i , "year" ] ) ,
                  this_data_file ,
                  sep = "|" ,
                  lower.case.names = TRUE ,
                  append = TRUE ,
                  #nrow.check = 70000
                  colClasses = ifelse( grepl( "num" , codebook$Tipo , ignore.case = TRUE ) , "numeric" , "character" )
                ) )
              file.remove( this_data_file )

            } else {

              suppressMessages(
                tryCatch(
                  DBI::dbWriteTable(
                    db,
                    ifelse( length( tabelas ) > 1 , tolower( gsub( "\\..*" , "" , basename( this_data_file ) ) ) , paste0( this_table_type , "_" , catalog[ i , "year" ] ) ),
                    this_data_file ,
                    sep = "|" ,
                    lower.case.names = TRUE ,
                    overwrite = TRUE ,
                    temporary = TRUE ,
                    colClasses = ifelse( grepl( "num" , codebook$Tipo , ignore.case = TRUE ) , "numeric" , "character" )
                  ) , error = function(e) {
                    cat( "removing non-utf8 characters\r" )
                    cleaned_data_file <- remove_nonutf8_censo_escolar( this_data_file ) ;
                    db <- DBI::dbConnect( MonetDBLite::MonetDBLite() , catalog[ i , 'dbfolder' ] )
                    DBI::dbWriteTable(
                      db,
                      ifelse( length( tabelas ) > 1 , tolower( gsub( "\\..*" , "" , basename( this_data_file ) ) ) , paste0( this_table_type , "_" , catalog[ i , "year" ] ) ),
                      cleaned_data_file ,
                      sep = "|" ,
                      lower.case.names = TRUE ,
                      overwrite = TRUE ,
                      temporary = TRUE ,
                      colClasses = ifelse( grepl( "num" , codebook$Tipo , ignore.case = TRUE ) , "numeric" , "character" )
                    )
                  } )
              )

              file.remove( this_data_file )

              if ( j == length( tabelas ) & length( tabelas ) > 1 ) {

                field.names <- data.frame( tablename = NULL , field = NULL )
                for ( tabela in tolower( gsub( "\\..*" , "" , basename( tabelas ) ) ) ) {
                  field.names <- rbind( field.names , data.frame( tablename = tabela , field = DBI::dbListFields( db , tabela ) , stringsAsFactors = FALSE ) )
                }

                field.names <- reshape2::dcast( field.names , field ~ tablename , value.var = "field" )

                std.string <- sapply( seq_along( tabelas ) , FUN = function( x ) {
                  paste0( "SELECT ",
                          paste0( ifelse( is.na( field.names[ , x + 1 ] ) , paste( "NULL AS" , field.names[ , 1 ] ) , field.names[ , 1 ] ) , collapse = " , " ),
                          " FROM ",  colnames( field.names ) [ x + 1 ] )
                }  )

                create.stacked <- paste0( "CREATE TABLE " , paste0( this_table_type , "_" , catalog[ i , "year" ] ) , " AS ( " ,
                                          paste0( std.string , collapse = " ) UNION ALL ( " ) ,
                                          " ) WITH DATA" )

                DBI::dbSendQuery( db , create.stacked )

                DBI::dbSendQuery( db , paste0( " DROP TABLE " , paste0( colnames( field.names )[ - 1 ] , collapse = " ; DROP TABLE " ) , " ;" ) )

              }

            }

            cat( tolower( gsub( "\\..*" , "" , basename( this_data_file ) ) ), "stored at" , paste0( this_table_type , "_" , catalog[ i , "year" ] ) , "\r\n\r" )

          }

        }

        catalog[ i , 'case_count' ] <- DBI::dbGetQuery( db , paste0( "SELECT COUNT(*) FROM matricula_" , catalog[ i , "year" ] ) )[ 1 , 1 ]

      }

      # disconnect from the current monet database
      DBI::dbDisconnect( db , shutdown = TRUE )

      # delete the temporary files?  or move some docs to a save folder?
      suppressWarnings( file.remove( tf ) )

      # remove raw data files already loaded
      for ( rm.dir in grep( "/DADOS/.*", unzipped_files , value = TRUE , ignore.case = TRUE ) ) {
        suppressWarnings( unlink( rm.dir , recursive = TRUE )  )
      }

      cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'dbfolder' ] , "'\r\n\n" ) )

    }

    catalog

  }



remove_nonutf8_censo_escolar <-
  function( infile ){

    tf_a <- tempfile()

    outcon <- file( tf_a , "w" )

    incon <- file( infile , "r" )

    while( length( line <- readLines( incon , 1 , warn = FALSE ) ) > 0 ) writeLines( iconv( line , "" , "ASCII//TRANSLIT" , sub = " " ) , outcon )

    close( incon )

    close( outcon )

    tf_a
  }

read_excel_metadata <- function( metadata_file , table_type ) {

  quietly_sheets <- purrr::quietly(readxl::excel_sheets)
  quietly_readxl <- purrr::quietly(readxl::read_excel)

  sheet_name <- grep( table_type, iconv( quietly_sheets( metadata_file )$result , from = "utf8" , to = "ASCII//TRANSLIT" ) , ignore.case = TRUE )
  codebook <-  data.frame( quietly_readxl( metadata_file , sheet = sheet_name , skip = 0 )$result , stringsAsFactors = FALSE )
  codebook <- codebook [ which( codebook[ , 1 ] == "N" ): nrow(codebook) , ]
  colnames( codebook ) <- codebook[ 1 , ]
  codebook <- codebook[ -1 , ]
  colnames( codebook ) <- iconv( colnames( codebook ) , from = "utf8" , to = "ASCII//TRANSLIT" )
  codebook <- tryCatch(
    codebook <- codebook[ !is.na( codebook[ , 2 ] ) , c( "Nome da Variavel" , "Tipo" , "Tam.(1)" ) ] ,
    error = function( e ) {

      possible.names <- list( c( "Nome da Variavel" , "Tipo" , "Tam. (1)" ) , c( "Nome novo da Variavel" , "Tipo" , "Tam. (1)" ), c( "Nome novo da Variavel" , "Tipo" , "Tam.(1)" ) )

      for( name_set in possible.names ) {

        if ( all( name_set %in% colnames( codebook ) ) ) {

          codebook <- codebook[ !is.na( codebook[ , 2 ] ) , name_set ]
          colnames( codebook ) <- c( "Nome da Variavel" , "Tipo" , "Tam.(1)" )

          return( codebook )

        }

      }

    } )
  codebook$`Nome da Variavel` <- tolower( codebook$`Nome da Variavel` )
  codebook[ is.na( codebook$Tipo ) , "Tipo" ] <- "Char"
  codebook[ is.na( codebook$Tipo ) , "Tam.(1)" ] <- 4
  codebook[ grepl( "^co_curso_[1-9]" , codebook$`Nome da Variavel` ) , c( "Tipo" ) ] <- "Char"
  codebook[ grepl( "^co_curso_[1-9]" , codebook$`Nome da Variavel` ) , c( "Tam.(1)" ) ] <- 10

  codebook

}
