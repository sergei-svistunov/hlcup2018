#include <thread>
#include <fstream>
#include <unistd.h>
#include "db/DB.h"
#include "Server.h"

int main(int argc, char *argv[]) {
/*    std::thread monitorThr([]() {
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wmissing-noreturn"
        while (true) {
            std::ifstream infile("/proc/self/status");
            std::string line, out;
            while (std::getline(infile, line)) {
                if (line.find("VmSize") != std::string::npos ||
                    line.find("VmRSS") != std::string::npos ||
                    line.find("VmPeak") != std::string::npos)
                    out += line + "\t";
            }

            std::cerr << out << std::endl;

            sleep(30);
        }
#pragma clang diagnostic pop
    });
    monitorThr.detach();*/

    hlcup::DB db;

    const char *port, *path;
    if (argc < 2) {
        port = "8080";
    } else {
        port = argv[1];
    }

    if (argc < 3) {
        path = "/home/svistunov/CLionProjects/hlcup2018/tank/data/data/data.zip";
    } else {
        path = argv[2];
    }
    db.Load(path);

    hlcup::Server server(&db);
    server.Run(port);

    return 0;
}