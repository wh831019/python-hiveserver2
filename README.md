python-hiveserver2
==================

Python thrift binding for HiveServer2 and simple query example.

The current binding files are compiled against Apache Hive 0.12 & Thrift 0.9.

##Usage
```bash
git clone https://github.com/lanyuyang/python-hiveserver2.git
cd python-hiveserver2

# 1) install python-sasl
cd ext-py/sasl-0.1.1
python setup.py install
#or
sudo python setup.py install

# if you got build error, try to install the following Linux packages & try again
# libsasl2-2, libsasl2-dev, python-dev

# 2) install python-hs2
cd ../../
python setup.py install
#or
sudo python setup.py install

# 3) try querying using example.py
# config your hiveserver information in example/example.py
vim example/example.py
# fill username, password, host, port, test_hql, etc.

# then check it out
python example/example.py

```
