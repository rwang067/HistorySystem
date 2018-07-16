#ifndef GRAPHWALKER_FILENAMES_DEF
#define GRAPHWALKER_FILENAMES_DEF

#include <fstream>
#include <fcntl.h>
#include <string>
#include <sstream>
#include <stdlib.h>
#include <errno.h>
#include <unistd.h>
#include <vector>
#include <sys/stat.h>

#include "api/datatype.hpp"
#include "logger/logger.hpp"

static std::string intervalname( std::string basefilename, int invlid ){
    std::stringstream ss;
    ss << basefilename;
    ss << "_invl/invl";
    ss << "_" << invlid << ".outlink";
    return ss.str();
}

static std::string blockname( std::string basefilename, int blockid ){
    std::stringstream ss;
    ss << basefilename;
    ss << "_block/block";
    ss << "_" << blockid << ".outlink";
    return ss.str();
}

/**
 * Configuration file name
 */
static std::string filename_config() {
    char * chi_root = getenv("GRAPHCHI_ROOT");
    if (chi_root != NULL) {
        return std::string(chi_root) + "/conf/graphchi.cnf";
    } else {
        return "conf/graphwalker.cnf";
    }
}

/**
 * Configuration file name - local version which can
 * override the version in the version control.
 */
static std::string filename_config_local() {
    char * chi_root = getenv("GRAPHCHI_ROOT");
    if (chi_root != NULL) {
        return std::string(chi_root) + "/conf/graphwalker.local.cnf";
    } else {
        return "conf/graphwalker.local.cnf";
    }
}


// static bool file_exists(std::string sname);
// static bool file_exists(std::string sname) {
//     int tryf = open(sname.c_str(), O_RDONLY);
//     if (tryf < 0) {
//         return false;
//     } else {
//         close(tryf);
//         return true;
//     }
// }

#endif