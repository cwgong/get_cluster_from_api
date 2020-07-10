def get_keywords_from_merged_cluster(words):
    x = {}

    for word in words:
        if word not in x.keys():
            x[word] = 1
        else:
            x[word] += 1

    x_ = sorted(x.items(), key=lambda d: d[1], reverse=True)

    # 不应该用 dict，打印出来会没有顺序
    x__ = {}
    for word, count in x_:
        print(word,' ',count)
        x__[word] = count
    return x__


words = [1,2,3,4,5,6,1,3,4,5,2,2,2,1,2,3,4,5,1,2,3,4,5,13,2,3]
print(words)

get_keywords_from_merged_cluster(words)