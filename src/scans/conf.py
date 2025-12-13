VOLUME_THRESHOLD = 10_00_00

filters_dict = {
    1: 4.99,  # close today ≥ close 1 day ago * 1.05
    22: 10,  # close today ≥ close 22 days ago * 1.10
    67: 20,  # close today ≥ close 67 days ago * 1.22
    # 125: 60,  # close today ≥ close 125 days ago * 1.60
}
