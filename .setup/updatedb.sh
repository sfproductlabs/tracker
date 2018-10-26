#!/bin/bash
files="schema.*.cql"
regex="schema\.([0-9]+)\.cql"
current_version=$(cqlsh --ssl --no-color -t -e "select * from sfpla.sequences;" | grep DB_VER | grep -oEi [0-9]+)
if [ -z "${current_version}" ]; then
    current_version=0
fi
echo "DB Version @ $current_version"
for f in $files
do
    if [[ $f =~ $regex ]]
    then	
        count="${BASH_REMATCH[1]}"
        if (($current_version < $count))
        then
            echo "Installing $count"
            bash -c "cqlsh --ssl -f $f"
        fi	
    fi
done
