mspider
=======

mspider 是一个简单的爬虫，使用线程池完成。
使用：
测试新浪三级页面：  
python mspider.py -u http://www.sina.com.cn -d 3 --dbfile test.db --thread 200  
  
自测(使用手机版百度)：  
python mspider.py --testself  

测试testdoc:  
python -m doctest -v fetchpage.py  
python -m doctest -v mthreadpool2.py  
python -m doctest -v mspider.py  

