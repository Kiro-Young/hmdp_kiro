package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;

import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Override
    public Result queryById(Long id) {
        // 缓存穿透
        //Shop shop = queryWithPassThrough(id);

        // 互斥锁解决缓存击穿
        Shop shop = queryWithMutex(id);
        if (shop == null) {
            return Result.fail("商铺不存在");
        }
        return Result.ok(shop);
    }

    private Shop queryWithMutex(Long id) {
        String key = CACHE_SHOP_KEY + id;
        // 从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 判断缓存是否命中
        if (StrUtil.isNotBlank(shopJson)) {
            // 命中，直接返回缓存数据
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        // 思路有点点小绕；isNotBlank为false时，可能为空或null；不等于null即是空即是缓存穿透
        if (shopJson != null) {
            // 缓存穿透，返回空值
            return null;
        }

        // 未命中，实现缓存重建
        // 尝试获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop;
        try {
            boolean isLock = tryLock(lockKey);
            // 判断是否获取锁成功
            if (!isLock) {
                // 获取锁失败，休眠并重试
                Thread.sleep(50);
                // 重试
                return queryWithMutex(id);
            }

            // 成功，根据id查询数据库
            // 注意：获取锁成功应该再次检测Redis缓存是否存在，做DoubleCheck；如果存在则无需重建缓存
            shop = getById(id);

            // 模拟重建延迟
//            Thread.sleep(3000);

            // 判断数据库是否命中
            if (shop == null) {
                // 未命中写入空值，防止缓存穿透
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            // 存在，写入Redis，返回数据
            stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            // 释放互斥锁
            unlock(lockKey);
        }

        // 返回
        return shop;
    }

    private Shop queryWithPassThrough(Long id) {
        String key = CACHE_SHOP_KEY + id;
        // 从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 判断缓存是否命中
        if (StrUtil.isNotBlank(shopJson)) {
            // 命中，直接返回缓存数据
            return JSONUtil.toBean(shopJson, Shop.class);
        }
        // 思路有点点小绕；isNotBlank为false时，可能为空或null；不等于null即是空即是缓存穿透
        if (shopJson != null) {
            // 缓存穿透，返回空值
            return null;
        }

        // 未命中，查询数据库
        Shop shop = getById(id);
        // 判断数据库是否命中
        if (shop == null) {
            // 未命中写入空值，防止缓存穿透
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        // 存在，写入Redis，返回数据
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(shop), CACHE_SHOP_TTL, TimeUnit.MINUTES);
        return shop;
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 5, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        // 靠id更新，id不能为空
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("商铺id不能为空");
        }
        // 更新数据库
        updateById(shop);
        // 删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }
}
