import attr


@attr.s
class RangeQuery:

    @classmethod
    def SpatialRangeQuery(self, spatialRDD, rangeQueryWindow, considerBoundaryIntersection, usingIndex):

        res = spatialRDD.sparkContext._jvm.\
            RangeQuery.SpatialRangeQuery(
            spatialRDD._srdd,
            rangeQueryWindow.create_java_object(spatialRDD._jvm),
            considerBoundaryIntersection,
            usingIndex
        )

        return res
