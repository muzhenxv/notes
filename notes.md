# 图片/视频base64编解码

```python
import cv2
import base64
import numpy as np
import traceback

def getFrames(url, frame_ids):
    """
    python版本抽帧与转换base64编码代码如下，有几个注意点：
    1. 如果抽帧失败，返回空字符串''
    2. 对于参数中的指定帧数，不强制要求视频本身必然包含，例如指定帧数为2,30，但视频只有20帧，则只返回第2帧即可
    3. 如果按照入参中的指定帧数参数进行抽帧后，没有抽取到任何一帧，则直接返回第一帧
    4. 该函数对视频和图片通用
    5. 必须用png，保证无损转换
    """
    try:
        frame_ids = [int(c.strip()) for c in frame_ids.split(',')]
        cap = cv2.VideoCapture(url)
        retval, i, imgs = True, 0, []
        while retval:
            retval, image = cap.read()
            if i == 0:
                first_image = image
            if (not retval) or (i >= max(frame_ids)):
                break
            i += 1
            if i in frame_ids:
                imgs.append(image)
        cap.release()
        
        print(imgs)
        if len(imgs) == 0:
            imgs.append(first_image)

        b64s = []
        for image in imgs:
            retval, buffer = cv2.imencode('.png', image)
            b64 = base64.b64encode(buffer).decode()
            b64s.append(b64)
        return '|'.join(b64s)
    except Exception as e:
        print(traceback.format_exc())
        return ''
    
def parseB64(a):
    jpg_original = base64.b64decode(a)

    jpg_as_np = np.frombuffer(jpg_original, dtype=np.uint8)
    image_buffer = cv2.imdecode(jpg_as_np, flags=1)
    return image_buffer

a = getFrames(url, '1,30')
```

# keras多线程机制与tornado/flask多线程冲突

https://blog.csdn.net/qq_39564555/article/details/95475871

# hive/pyspark窗口函数

当为聚合函数时，如果使用order by，则窗口计算只到当前行！

            imgs.append(first_image)juh e
            imgs.append(first_image)
            
# coalesce导致窄依赖问题

假设RDD有N个分区，需要重新划分成M个分区：

N < M: 一般情况下N个分区有数据分布不均匀的状况，利用HashPartitioner函数将数据重新分区为M个，这时需要将shuffle设置为true。因为重分区前后相当于宽依赖，会发生shuffle过程，此时可以使用coalesce(shuffle=true)，或者直接使用repartition()。

如果N > M并且N和M相差不多(假如N是1000，M是100): 那么就可以将N个分区中的若干个分区合并成一个新的分区，最终合并为M个分区，这是前后是窄依赖关系，可以使用coalesce(shuffle=false)。

如果 N> M并且两者相差悬殊: 这时如果将shuffle设置为false，父子ＲＤＤ是窄依赖关系，他们同处在一个Ｓｔａｇｅ中，就可能造成spark程序的并行度不够，从而影响性能，如果在M为1的时候，为了使coalesce之前的操作有更好的并行度，可以将shuffle设置为true。

# sql序列保序拼接
```sql
                   regexp_replace(
                     concat_ws('|',
                       sort_array(
                         collect_list(
                           concat_ws(':',lpad(cast(rk as string),2,'0'),cast(spandays as string))
                         )
                       )
                     ),
                   '\\d+\\:','') day_list
```
