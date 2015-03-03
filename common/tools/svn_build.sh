#!/bin/bash
#
if [ x$1 == x"" ];
then
    TIME_TAG=$(date +%Y%m%d%H%M)
    change_list_name=temp.$TIME_TAG
else
    change_list_name=$1
fi

temp_file="/tmp/svn_build_change_list_$change_list_name"
temp_comment_file="/tmp/svn_build_comment_$change_list_name"
temp_modified_change_file="/tmp/svn_build_modified_change_file_$change_list_name"

rm -rf $temp_file $temp_comment_file $temp_modified_change_file

echo "comment:" >> $temp_file
echo "" >> $temp_file
echo "changes:" >> $temp_file
 `svn st | grep -v "^?" >> $temp_file `

vim $temp_file
#cat $temp_file

flag=1
while read line
do
    if [ x"$line" = x"comment:" ]
    then
        flag=1
        continue
    fi
    if [ x"$line" = x"changes:" ]
    then
        flag=2
        continue
    fi
    if [ x"$flag" = x"1" ]
    then
        echo $line >> $temp_comment_file
    else
        echo $line | cut -d' ' -f2 >> $temp_modified_change_file 
        #echo $line >> $temp_modified_change_file
    fi
done < $temp_file

echo "" >> $temp_comment_file
echo "Changes:" >> $temp_comment_file
cat $temp_modified_change_file >> $temp_comment_file

changes=`cat $temp_modified_change_file`
#echo "add '"$changes"' to change list '"$change_list_name"'"
svn changelist $change_list_name $changes > /dev/null
svn ci --changelist $change_list_name -F $temp_file  #$temp_comment_file

rm -rf $temp_file $temp_comment_file $temp_modified_change_file
