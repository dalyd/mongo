// sleep.cpp

/**
*    Copyright (C) 2015 10gen Inc.
*
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*
*    As a special exception, the copyright holders give permission to link the
*    code of portions of this program with the OpenSSL library under certain
*    conditions as described in each individual source file and distribute
*    linked combinations including the program with the OpenSSL library. You
*    must comply with the GNU Affero General Public License in all respects for
*    all of the code used other than as permitted herein. If you modify file(s)
*    with this exception, you may extend this exception to your version of the
*    file(s), but you are not obligated to do so. If you do not wish to do so,
*    delete this exception statement from your version. If you delete this
*    exception statement from all source files in the program, then also delete
*    it in the license file.
*/

#include "mongo/platform/basic.h"

#include "mongo/base/init.h"
#include "mongo/db/commands.h"

namespace mongo {

    using std::string;
    using std::stringstream;

    class CPULoadCommand : public Command {
    public:
        CPULoadCommand() : Command("cpuload") {}
        virtual bool slaveOk() const { return true; }
        virtual bool isWriteCommandForConfigServer() const { return false; }
        virtual void help( stringstream &help ) const {
            help << "{ cpuload : 1, factor : 1 } Runs a straight CPU load. Length of execution scaled by factor. Puts no additional load on the server beyond the cpu use";
        }
        virtual void addRequiredPrivileges(const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           std::vector<Privilege>* out) {} // No auth required
        virtual bool run(OperationContext* txn,
                         const string& badns,
                         BSONObj& cmdObj,
                         int,
                         string& errmsg,
                         BSONObjBuilder& result) {
            double factor = 1;
            long long limit = 10000;
            if (cmdObj["factor"].isNumber()) {
                factor = cmdObj["factor"].number();
            }
            limit = limit * factor;
            volatile uint64_t lresult;
            uint64_t x = 0;
            for (long long i = 0; i < limit; i++)
                {
                    x+= 1;
                }
            lresult = x;
            return true;
        }
    };

    MONGO_INITIALIZER_WITH_PREREQUISITES(RegisterCPULoadCommand, ("GenerateInstanceId"))
        (InitializerContext* context) {
        // Leaked intentionally: a Command registers itself when constructed
        new CPULoadCommand();
        return Status::OK();
    }

}  // namespace mongo
