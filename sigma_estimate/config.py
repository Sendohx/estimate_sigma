# -*- coding:UTF-8 -*-

from datetime import datetime

class config:
    '''配置参数'''
    def root_config(self):
        print('开始配置路径\n')
        self.root = str(input('请输入文件存放路径：\n')) #

    def object_config(self):
        ''''''
        print('开始配置回测参数\n')

        self.start_date = str(input('请输入回测起始日期(格式：yyyymmdd)：\n'))
        self.end_date = datetime.today().strftime('%Y%m%d') # 回测结束日期

        object_type = str(input('请说明标的类型（输入 index 或 stock）:\n'))
        self.object_type = object_type
        if object_type == 'index':
            self.index_list = []  # 指数标的代码列表
            while True:
                index = str(input("请输入指数代码（按Enter键结束输入）: \n"))
                if index == '':
                    break
                self.index_list.append(index)
        elif object_type == 'stock':
            self.stock_list = []  # 指数标的代码列表
            while True:
                stock = str(input("请输入股票代码（按Enter键结束输入）: \n"))
                if stock == '':
                    break
                self.index_list.append(stock)
        else:
            print('输入错误,请重新运行\n')

    def param_config(self):
        ''''''
        print('开始配置模型超参数\n')

        self.test_window = []
        while True:
            window = input('请输入想要测试的窗口大小（按Enter键结束输入）: \n')
            if window == '':
                break
            self.test_window.append(int(window))

        self.lamb = []
        while True:
            lamb = input('请输入想要测试的EWMA模型的lambda参数值（按Enter键结束输入）: \n')
            if lamb == '':
                break
            self.lamb.append(float(lamb))

        self.p = []
        while True:
            p = input('请输入想要测试的arch效应阶数（按Enter键结束输入）：\n')
            if p == '':
                break
            self.p.append(int(p))

        self.q = []
        while True:
            q = input('请输入想要测试的garch效应阶数（按Enter键结束输入）：\n')
            if q == '':
                break
            self.q.append(int(q))

        self.return_type = []
        while True:
            ret = str(input('请输入想要测试的日收益率类型（return, log_return, abs_return; 按Enter键结束输入）：\n'))
            if ret == '':
                break
            self.return_type.append(ret)

    def run(self):
        self.root_config()
        self.object_config()
        self.param_config()
        print('参数配置完成\n')
