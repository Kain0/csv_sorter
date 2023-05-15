import csv
import os
import time


class Sorter:
    chunk_size = 2048

    def __init__(self, path):
        self.size = 0
        self.sort_index = 0
        self.path = path
        self.clear_all_temp_files(3)
        self.temp_files_count = 0
        self.is_string = False
        self.clear_all_temp_files(3)

    def sort(self, index, is_str=False):
        self.is_string = is_str
        self.sort_index = index
        self.make_start_chunks()
        print("found elements: ", self.size)
        step = 2
        while self.chunk_size < self.size:
            self.sum_up()
            print("step", step, "done (merge)", "| chunk_size: ", self.chunk_size)
            step += 1
            self.chunk_size *= 2
            if self.chunk_size < self.size:
                self.split_up()
        self.sum_up()
        print("step", step, "done (merge)", "| chunk_size: ", self.chunk_size)
        try:
            os.remove(self.path + "_sorted.csv")
        except OSError:
            pass
        os.rename("2temp.csv", self.path + "_sorted.csv")
        self.clear_all_temp_files(2)

    def check(self):
        with open(self.path + "_sorted.csv", 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            last = None
            count = 0
            count_valid = 0
            for row in reader:
                count += 1
                if not last:
                    last = row
                    continue

                # print(row[self.sort_index])
                if self.compare(last, row):
                    # print(row[self.sort_index])
                    count_valid += 1
                last = row.copy()
            if count == self.size and count == count_valid + 1:
                print("sort finished!")
            else:
                print("sort result doesn't seem alright :(")

    def sum_up(self):  # like merge

        self.clear_temp_file(2)
        with open("0temp.csv", 'r', encoding='utf-8') as f1:
            temp0 = csv.reader(f1)
            with open("1temp.csv", 'r', encoding='utf-8') as f2:
                temp1 = csv.reader(f2)
                with open("2temp.csv", 'a+', encoding='utf-8', newline='\n') as f3:
                    writer = csv.writer(f3)

                    next_row_0 = self.next_row(temp0)
                    next_row_1 = self.next_row(temp1)

                    k = 0
                    j = 0
                    while next_row_0 is not None or next_row_1 is not None:
                        if k >= self.chunk_size and j >= self.chunk_size:
                            k = 0
                            j = 0
                            continue
                        if k >= self.chunk_size > j and next_row_1 is not None:
                            writer.writerow(next_row_1)
                            next_row_1 = self.next_row(temp1)
                            j += 1
                            continue
                        if j >= self.chunk_size > k and next_row_0 is not None:
                            writer.writerow(next_row_0)
                            next_row_0 = self.next_row(temp0)
                            k += 1
                            continue

                        if next_row_0 is None:
                            writer.writerow(next_row_1)
                            next_row_1 = self.next_row(temp1)
                            j += 1
                            continue
                        if next_row_1 is None:
                            writer.writerow(next_row_0)
                            next_row_0 = self.next_row(temp0)
                            k += 1
                            continue

                        # usual case
                        if self.compare(next_row_0, next_row_1):
                            writer.writerow(next_row_0)
                            next_row_0 = self.next_row(temp0)
                            k += 1
                        else:
                            writer.writerow(next_row_1)
                            next_row_1 = self.next_row(temp1)
                            j += 1

    def split_up(self):
        try:
            self.clear_temp_file(0)
            self.clear_temp_file(1)
            with open("2temp.csv", 'r', encoding='utf-8') as f:
                reader = csv.reader(f)

                elements_count = 0
                chunk_index = 0

                with open("0temp.csv", 'a+', encoding='utf-8', newline='\n') as f0:
                    temp0 = csv.writer(f0)
                    with open("1temp.csv", 'a+', encoding='utf-8', newline='\n') as f1:
                        temp1 = csv.writer(f1)

                        for row in reader:
                            if elements_count >= self.chunk_size:
                                chunk_index = (chunk_index + 1) % 2
                                elements_count = 0
                            elements_count += 1
                            self.write_row(row, temp0, temp1, chunk_index)
        except OSError:
            print("Error: on splitting")
            exit(-1)

    def make_start_chunks(self):
        try:
            with open(self.path, 'r', encoding='utf-8') as f:
                reader = csv.reader(f)

                chunk = []
                chunk_index = 0
                for row in reader:
                    if not row:
                        continue
                    if len(chunk) == self.chunk_size:
                        if self.is_string:
                            chunk.sort(key=lambda x: str(x[self.sort_index]))
                        else:
                            chunk.sort(key=lambda x: float(x[self.sort_index]))
                        self.write_chunk(chunk, chunk_index)
                        chunk = []
                        chunk_index = (chunk_index + 1) % 2
                    self.size += 1
                    chunk.append(row)

                if chunk:
                    if self.is_string:
                        chunk.sort(key=lambda x: str(x[self.sort_index]))
                    else:
                        chunk.sort(key=lambda x: float(x[self.sort_index]))
                    self.write_chunk(chunk, chunk_index)
            print("step 1 done (sort chunks)")
        except OSError:
            print("Error:  on creating start chunks")
            exit(-1)
        except:
            print("Error:  on creating start chunks. Maybe wrong types")
            exit(-1)

    def write_chunk(self, chunk, index):
        try:
            if index == self.temp_files_count:
                self.temp_files_count += 1
            with open(str(index) + "temp.csv", 'a+', encoding='utf-8', newline='\n') as f:
                writer = csv.writer(f)
                for row in chunk:
                    writer.writerow(row)
        except OSError:
            print("Error: cannot  write in csv")
            exit(-1)

    @staticmethod
    def write_row(row, file0, file1, index):
        try:
            if index == 0:
                file0.writerow(row)
            else:
                file1.writerow(row)
        except OSError:
            print("Error: cannot  write in csv")
            exit(-1)

    @staticmethod
    def next_row(file):
        try:
            row = next(file)
            return row
        except StopIteration:
            return None

    @staticmethod
    def clear_temp_file(index):
        try:
            open(str(index) + 'temp.csv', 'w').close()
        except OSError:
            print("Error: cannot open clear temp file")
            exit(-1)

    @staticmethod
    def clear_all_temp_files(count):
        try:
            for index in range(count):
                open(str(index) + 'temp.csv', 'w').close()
        except OSError:
            print("Error: cannot open clear temp files")
            exit(-1)

    def compare(self, row1, row2):
        try:
            if self.is_string:
                return row1[self.sort_index] <= row2[self.sort_index]
            else:
                return float(row1[self.sort_index]) <= float(row2[self.sort_index])
        except:
            print("Error: cannot compare. maybe wrong types:")
            print(row1)
            print(row2)
            exit(-1)


if __name__ == '__main__':

    print("Write info about sort in format: file_name index type")
    is_valid = False
    name, i, data_type = None, None, None
    while not is_valid:
        name, i, data_type = input(":").split()
        data_type = data_type == "s"
        if str(i).isdigit() and int(i) >= 0:
            is_valid = True
            i = int(i)
        else:
            print("Error: index is wrong")

    s = Sorter(name)
    s.chunk_size = 2 ** 20

    start = time.time()
    s.sort(i, data_type)
    end = time.time() - start

    s.check()
    print("time: ", end)
