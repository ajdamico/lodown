get_catalog_sim <-
  function( data_name = "sim" , output_dir , ... ){

    # Part 1: specific tables
    sim_portal <- "ftp://ftp.datasus.gov.br/dissemin/publicos/SIM/CID10/DOFET/"

    filenames <- RCurl::getURL(sim_portal, verbose=FALSE, ftp.use.epsv=FALSE, dirlistonly = TRUE , crlf = FALSE )
    filenames <- strsplit( filenames, "\r*\n")[[1]]

    full_url = paste( sim_portal, filenames, sep = "")

    year_lines <- substr( filenames , 6, 7 )

    time.thresh <- as.numeric(year_lines) < 90
    year_lines [ time.thresh ] <- paste0( "20" , year_lines [ time.thresh ] )
    year_lines [ !time.thresh ] <- paste0( "19" , year_lines [ !time.thresh ] )

    tod_lines <- substr( filenames , 3, 5 )
    tod_lines[ tod_lines == "EXT" ] <- "externa"
    tod_lines[ tod_lines == "FET" ] <- "fetal"
    tod_lines[ tod_lines == "INF" ] <- "infantil"
    tod_lines[ tod_lines == "MAT" ] <- "materna"

    catalog_pt1 <-
      data.frame(
        type = tod_lines,
        uf = NA,
        year = year_lines ,
        full_url = full_url ,
        db_tablename = paste0( tod_lines , year_lines ) ,
        dbfolder = paste0( output_dir , "/MonetDB" ) ,
        output_folder = paste( output_dir , tod_lines , sep = "/" ) ,
        stringsAsFactors = FALSE
      )

    # Part 2: general table
    sim_portal <- "ftp://ftp.datasus.gov.br/dissemin/publicos/SIM/CID10/DORES/"

    filenames <- RCurl::getURL(sim_portal, verbose=FALSE, ftp.use.epsv=FALSE, dirlistonly = TRUE , crlf = FALSE )
    filenames <- strsplit( filenames, "\r*\n")[[1]]

    full_url = paste( sim_portal, filenames, sep = "")

    year_lines <- substr( filenames , 5, 8 )

    catalog_pt2 <-
      data.frame(
        type = "geral" ,
        uf = substr( filenames , 3, 4 ),
        year = year_lines ,
        full_url = full_url ,
        db_tablename = paste0( "geral", year_lines ) ,
        dbfolder = paste0( output_dir , "/MonetDB" ) ,
        output_folder = paste( output_dir , year_lines , sep = "/" ) ,
        stringsAsFactors = FALSE
      )


    # create full catalog
    catalog <- rbind( catalog_pt1 , catalog_pt2 )

    catalog[ with( catalog, order( type, uf , year ) ) , ]

  }


lodown_sim <-
  function( data_name = "sim" , catalog , ... ){

    tf <- tempfile()

    for ( i in seq_len( nrow( catalog ) ) ){

      # open the connection to the monetdblite database
      db <- DBI::dbConnect( MonetDBLite::MonetDBLite() , catalog[ i , 'dbfolder' ] )

      # download the file
      cachaca( catalog[ i , "full_url" ] , tf , mode = 'wb' )

      data <- read.dbc::read.dbc( file = tf )
      colnames(data) <- tolower( colnames(data) )
      colnames(data) [ grep( "natural", colnames( data ) , ignore.case = T ) ] <- "c_natural"

      DBI::dbWriteTable( conn = db, name = catalog[ i , 'db_tablename' ], value = data, row.names = FALSE , append = TRUE )

      file.remove( tf )
      rm( data )
      gc()

      cat( paste0( data_name , " catalog entry " , i , " of " , nrow( catalog ) , " stored in '" , catalog[ i , 'dbfolder' ] , "'\r\n\n" ) )

    }

    invisible( TRUE )

  }

