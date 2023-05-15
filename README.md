# csv_sorter

### метод сортировки  
используется улучшенная сортировка слиянием:  
1) используется лишь 3 вспомогательных файла. Их суммарный вес не превышает удвоенного веса изначального файла  
2) сначала сортируются чанки заданной длины в оперативной памяти. Остальные шаги - это слияние отсортированных 
последовательностей на диске.  
3) нет рекурсии  
4) сложность по времени - O(nlogn), сложность по памяти на диске O(n), сложность по памяти в ОЗУ: O(const)  

### запуск
1) запустить файл main.py  
2) ввести название файла, номер колонки (нумерация с нуля), тип данных ('s' - строки, другой ввод будет обработан, как int или float)  
3) нажать Enter  
4) пример запуска:  ![image](https://github.com/Kain0/csv_sorter/assets/53154029/7e2ee138-d752-477a-8a02-5978924da5f6)


### дополнительно
1) можно поиграться с chunk_size, чтобы обрабатывать больше данных в ОЗУ. Сейчас обрабатывается 2^20 записей.  